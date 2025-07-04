import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
from datetime import datetime, timedelta
import logging
import os
from dotenv import load_dotenv

from growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

warnings.filterwarnings("ignore", category=FutureWarning)
load_dotenv()
def get_db_connection():
    password = urllib.parse.quote_plus(os.environ['DB_PASSWORD'])
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    logging.info("✅ 数据库连接已建立。")
    return engine

def get_and_save_daily_order_rate_by_experiment(tag):
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"⚠️ 没有找到符合标签 '{tag}' 的实验数据！")
        return None

    experiment_name = experiment_data['experiment_name']
    start_date = experiment_data['phase_start_time'].date()
    end_date = experiment_data['phase_end_time'].date()

    engine = get_db_connection()
    table_name = f"tbl_report_subscribtion_rate_{tag}"

    # 建表语句
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        sub_date DATE,
        variation_id VARCHAR(255),
        group_user_count INT,
        new_subscribe_users INT,
        order_rate DOUBLE,
        experiment_tag VARCHAR(255)
    );
    """
    truncate_query = f"TRUNCATE TABLE {table_name};"

    with engine.connect() as conn:
        conn.execute(text(create_table_query))
        conn.execute(text(truncate_query))

    # SQL 查询
    sql = f"""
    WITH exp AS (
        SELECT user_id, variation_id, event_date
        FROM (
            SELECT user_id, variation_id, event_date,
                   ROW_NUMBER() OVER (PARTITION BY user_id, event_date ORDER BY event_date DESC) AS rn
            FROM flow_wide_info.tbl_wide_experiment_assignment_hi
            WHERE experiment_id = '{experiment_name}'
              AND event_date > '{start_date}' AND event_date <= '{end_date}'
        ) t
        WHERE rn = 1
    ),
    android_new AS (
        SELECT user_id, DATE(sub_date) AS dt
        FROM flow_wide_info.tbl_wide_business_subscribe_google_detail
        WHERE notification_type = 4
          AND sub_date > '{start_date}' AND sub_date <= '{end_date}'
    ),
    ios_new AS (
        SELECT user_id, DATE(sub_date) AS dt
        FROM flow_wide_info.tbl_wide_business_subscribe_apple_detail
        WHERE notification_type = 'SUBSCRIBED'
          AND sub_date > '{start_date}' AND sub_date <= '{end_date}'
    ),
    all_new_subscribe AS (
        SELECT user_id, dt FROM android_new
        UNION ALL
        SELECT user_id, dt FROM ios_new
    ),
    labeled_new_subscribe AS (
        SELECT
            e.variation_id,
            s.dt AS sub_date,
            s.user_id
        FROM all_new_subscribe s
        JOIN exp e ON s.user_id = e.user_id AND s.dt = e.event_date
    ),
    group_users AS (
        SELECT event_date AS sub_date, variation_id, COUNT(DISTINCT user_id) AS group_user_count
        FROM exp
        GROUP BY event_date, variation_id
    ),
    group_new_subscribe AS (
        SELECT sub_date, variation_id, COUNT(DISTINCT user_id) AS new_subscribe_users
        FROM labeled_new_subscribe
        GROUP BY sub_date, variation_id
    )
    SELECT
        g.sub_date,
        g.variation_id,
        g.group_user_count,
        COALESCE(n.new_subscribe_users, 0) AS new_subscribe_users,
        CASE WHEN g.group_user_count = 0 THEN 0
             ELSE ROUND(COALESCE(n.new_subscribe_users, 0) / g.group_user_count, 4) END AS order_rate,
        '{tag}' AS experiment_tag
    FROM group_users g
    LEFT JOIN group_new_subscribe n
      ON g.sub_date = n.sub_date AND g.variation_id = n.variation_id
    ORDER BY g.sub_date DESC, g.variation_id
    """

    df = pd.read_sql(sql, engine)
    print(df)

    # 写入目标表
    if not df.empty:
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table_name};"))  # 确保表为空
        df.to_sql(table_name, engine, index=False, if_exists='append')
        print(f"✅ {table_name} 数据已写入！")
    else:
        print("⚠️ 查询结果为空。")
    return df

if __name__ == "__main__":
    get_and_save_daily_order_rate_by_experiment("trans_pt")
