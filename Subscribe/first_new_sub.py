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
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_db_connection():
    password = urllib.parse.quote_plus(os.environ['DB_PASSWORD'])
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    logging.info("✅ 数据库连接已建立。")
    return engine

def get_and_save_first_subscribe_rate_by_experiment(tag):
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        logging.warning(f"⚠️ 没有找到符合标签 '{tag}' 的实验数据！")
        return None

    experiment_name = experiment_data['experiment_name']
    start_date = experiment_data['phase_start_time'].date()
    end_date = experiment_data['phase_end_time'].date()
    engine = get_db_connection()
    table_name = f"tbl_report_first_subscribe_rate_{tag}"

    # 建表+只清空一次
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        sub_date DATE,
        variation_id VARCHAR(255),
        experiment_tag VARCHAR(255),
        group_user_count INT,
        first_subscribe_users INT,
        first_subscribe_rate DOUBLE
    )
    UNIQUE KEY(sub_date, variation_id, experiment_tag)
    DISTRIBUTED BY HASH(sub_date)
    PROPERTIES (
        "replication_num" = "2"
    );
    """
    with engine.connect() as conn:
        conn.execute(text(create_table_query))
        conn.execute(text(f"TRUNCATE TABLE {table_name};"))

    # 批量循环所有日期
    cur_date = start_date
    while cur_date <= end_date:
        logging.info(f"🎯 处理 {cur_date} ...")
        # SQL内严选首订条件
        sql = f"""
        WITH exp AS (
            SELECT user_id, variation_id, event_date
            FROM (
                SELECT
                    user_id,
                    variation_id,
                    event_date,
                    ROW_NUMBER() OVER (PARTITION BY user_id, event_date ORDER BY timestamp_assigned ASC) AS rn
                FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                WHERE experiment_id = '{experiment_name}'
                  AND event_date = '{cur_date}'
            ) t
            WHERE rn = 1
        ),
        google_first_sub AS (
            SELECT user_id, MIN(DATE(sub_date)) AS first_sub_date
            FROM flow_wide_info.tbl_wide_business_subscribe_google_detail
            WHERE notification_type = 4 AND new_subscription = 1
              AND DATE(sub_date) = '{cur_date}'
            GROUP BY user_id
        ),
        apple_first_sub AS (
            SELECT user_id, MIN(DATE(sub_date)) AS first_sub_date
            FROM flow_wide_info.tbl_wide_business_subscribe_apple_detail
            WHERE notification_type = 'SUBSCRIBED' AND new_subscription = 1
              AND DATE(sub_date) = '{cur_date}'
            GROUP BY user_id
        ),
        union_first_sub AS (
            SELECT user_id, first_sub_date FROM google_first_sub
            UNION ALL
            SELECT user_id, first_sub_date FROM apple_first_sub
        ),
        first_subscribe_final AS (
            SELECT user_id, MIN(first_sub_date) AS first_sub_date
            FROM union_first_sub
            GROUP BY user_id
        ),
        labeled_first_subscribe AS (
            SELECT
                e.variation_id,
                f.first_sub_date AS sub_date,
                f.user_id
            FROM first_subscribe_final f
            JOIN exp e ON f.user_id = e.user_id AND f.first_sub_date = e.event_date
            WHERE f.first_sub_date = '{cur_date}'
        ),
        group_users AS (
            SELECT event_date AS sub_date, variation_id, COUNT(DISTINCT user_id) AS group_user_count
            FROM exp
            GROUP BY event_date, variation_id
        ),
        group_first_subscribe AS (
            SELECT sub_date, variation_id, COUNT(DISTINCT user_id) AS first_subscribe_users
            FROM labeled_first_subscribe
            GROUP BY sub_date, variation_id
        )
        SELECT
            g.sub_date,
            g.variation_id,
            '{tag}' AS experiment_tag,
            g.group_user_count,
            COALESCE(f.first_subscribe_users, 0) AS first_subscribe_users,
            CASE WHEN g.group_user_count = 0 THEN 0
                ELSE ROUND(COALESCE(f.first_subscribe_users, 0) / g.group_user_count, 4) END AS first_subscribe_rate
        FROM group_users g
        LEFT JOIN group_first_subscribe f
        ON g.sub_date = f.sub_date AND g.variation_id = f.variation_id
        ORDER BY g.sub_date DESC, g.variation_id
        """

        df = pd.read_sql(sql, engine)
        logging.info(f"🎉 {cur_date} 查询到 {len(df)} 条数据")
        if not df.empty:
            with engine.connect() as conn:
                for _, row in df.iterrows():
                    insert_sql = text(f"""
                    INSERT INTO {table_name} (sub_date, variation_id, experiment_tag, group_user_count, first_subscribe_users, first_subscribe_rate)
                    VALUES (:sub_date, :variation_id, :experiment_tag, :group_user_count, :first_subscribe_users, :first_subscribe_rate)
                    """)
                    conn.execute(insert_sql, dict(row))
            logging.info(f"✅ {cur_date} 数据已写入 {table_name}！")
        cur_date += timedelta(days=1)

if __name__ == "__main__":
    get_and_save_first_subscribe_rate_by_experiment("show_sub_ad")
