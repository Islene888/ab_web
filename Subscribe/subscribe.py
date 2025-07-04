import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
from growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag
from datetime import datetime, timedelta
import logging
import os
from dotenv import load_dotenv

warnings.filterwarnings("ignore", category=FutureWarning)
load_dotenv()
def get_db_connection():
    password = urllib.parse.quote_plus(os.environ['DB_PASSWORD'])
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    logging.info("✅ 数据库连接已建立。")
    return engine

def get_daily_subscribe_metrics_with_subscribe_rate(tag):
    print(f"🚀 开始获取每日订阅相关指标，标签: {tag}")
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"⚠️ 没有找到符合标签 '{tag}' 的实验数据！")
        return None

    experiment_name = experiment_data['experiment_name']
    start_date = experiment_data['phase_start_time'].date()
    end_date = experiment_data['phase_end_time'].date()
    print(f"📝 实验名称: {experiment_name}")
    print(f"⏰ 实验周期: {start_date} ~ {end_date}")

    engine = get_db_connection()
    table_name = f"tbl_report_subscribe_metrics_{tag}"

    create_table_query = f"""
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE IF NOT EXISTS {table_name} (
        event_date DATE,
        variation_id VARCHAR(255),
        experiment_user_count INT,
        new_subscribe_users INT,
        subscribe_rate DOUBLE,
        subscribe_orders INT,
        subscribe_repeat_orders INT,
        subscribe_renew_orders INT,
        subscribe_repurchase_rate DOUBLE,
        subscribe_renew_rate DOUBLE,
        flux_orders INT,
        flux_repeat_orders INT,
        flux_repurchase_rate DOUBLE,
        total_orders INT,
        repurchase_rate DOUBLE,
        experiment_tag VARCHAR(255)
    );
    """

    with engine.connect() as conn:
        for stmt in create_table_query.strip().split(';'):
            if stmt.strip():
                conn.execute(text(stmt))

    all_results = []
    day = start_date + timedelta(days=1)  # 从第二天开始，排除首日
    while day <= end_date:
        target_date = day.strftime('%Y-%m-%d')
        print(f"👉 [Info] 正在处理日期: {target_date}")
        sql = f"""
        WITH exp AS (
            SELECT user_id, variation_id
            FROM (
                SELECT user_id, variation_id,
                       ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_date DESC) AS rn
                FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                WHERE experiment_id = '{experiment_name}'
                  AND event_date = '{target_date}'
            ) t
            WHERE rn = 1
        ),
        subscribe_raw AS (
            SELECT
                s.user_id,
                e.variation_id,
                s.event_date,
                s.new_subscription,
                s.new_product_subscription
            FROM flow_event_info.tbl_app_event_subscribe s
            JOIN exp e ON s.user_id = e.user_id AND s.event_date = '{target_date}'
        ),
        flux_raw AS (
            SELECT
                c.user_id,
                e.variation_id,
                c.event_date,
                c.is_first_flux
            FROM flow_event_info.tbl_app_event_currency_purchase c
            JOIN exp e ON c.user_id = e.user_id AND c.event_date = '{target_date}'
        ),
        new_subscribe_users AS (
            SELECT variation_id, COUNT(DISTINCT user_id) AS new_subscribe_users
            FROM subscribe_raw
            WHERE new_subscription = TRUE
            GROUP BY variation_id
        ),
        experiment_users AS (
            SELECT variation_id, COUNT(DISTINCT user_id) AS experiment_user_count
            FROM exp
            GROUP BY variation_id
        )
        SELECT
            '{target_date}' AS event_date,
            eu.variation_id,
            eu.experiment_user_count,
            COALESCE(nsu.new_subscribe_users, 0) AS new_subscribe_users,
            CASE WHEN eu.experiment_user_count = 0 THEN 0
                 ELSE COALESCE(nsu.new_subscribe_users, 0) / eu.experiment_user_count END AS subscribe_rate,
            -- 订阅
            COUNT(sr.user_id) AS subscribe_orders,
            SUM(CASE WHEN sr.new_subscription = FALSE THEN 1 ELSE 0 END) AS subscribe_repeat_orders,
            SUM(CASE WHEN sr.new_product_subscription = FALSE THEN 1 ELSE 0 END) AS subscribe_renew_orders,
            CASE WHEN COUNT(sr.user_id) = 0 THEN 0
                 ELSE SUM(CASE WHEN sr.new_subscription = FALSE THEN 1 ELSE 0 END) / COUNT(sr.user_id) END AS subscribe_repurchase_rate,
            CASE WHEN COUNT(sr.user_id) = 0 THEN 0
                 ELSE SUM(CASE WHEN sr.new_product_subscription = FALSE THEN 1 ELSE 0 END) / COUNT(sr.user_id) END AS subscribe_renew_rate,
            -- 充值
            COUNT(fr.user_id) AS flux_orders,
            SUM(CASE WHEN fr.is_first_flux = 0 THEN 1 ELSE 0 END) AS flux_repeat_orders,
            CASE WHEN COUNT(fr.user_id) = 0 THEN 0
                 ELSE SUM(CASE WHEN fr.is_first_flux = 0 THEN 1 ELSE 0 END) / COUNT(fr.user_id) END AS flux_repurchase_rate,
            -- 总订单、复购率
            COUNT(sr.user_id) + COUNT(fr.user_id) AS total_orders,
            CASE WHEN (COUNT(sr.user_id) + COUNT(fr.user_id)) = 0 THEN 0
                 ELSE (SUM(CASE WHEN sr.new_subscription = FALSE THEN 1 ELSE 0 END) +
                       SUM(CASE WHEN fr.is_first_flux = 0 THEN 1 ELSE 0 END))
                      / (COUNT(sr.user_id) + COUNT(fr.user_id)) END AS repurchase_rate,
            '{tag}' AS experiment_tag
        FROM experiment_users eu
        LEFT JOIN new_subscribe_users nsu ON eu.variation_id = nsu.variation_id
        LEFT JOIN subscribe_raw sr ON eu.variation_id = sr.variation_id
        LEFT JOIN flux_raw fr ON eu.variation_id = fr.variation_id
        GROUP BY eu.variation_id, eu.experiment_user_count, nsu.new_subscribe_users;
        """
        df = pd.read_sql(sql, engine)
        if not df.empty:
            all_results.append(df)
        day += timedelta(days=1)

    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table_name};"))
        final_df.to_sql(table_name, engine, index=False, if_exists='append')
        print(f"✅ {table_name} 全量每日数据已写入！")
        print(final_df)
        return final_df
    else:
        print("⚠️ 查询结果为空。")
        return None

if __name__ == "__main__":
    get_daily_subscribe_metrics_with_subscribe_rate("trans_pt")
