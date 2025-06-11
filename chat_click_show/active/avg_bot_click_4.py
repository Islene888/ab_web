import sys
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

import logging
import os
from dotenv import load_dotenv

load_dotenv()
def get_db_connection():
    password = urllib.parse.quote_plus(os.environ['DB_PASSWORD'])
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    logging.info("✅ 数据库连接已建立。")
    return engine

def main(tag: str):
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        raise ValueError(f"⚠️ 没有找到实验标签 {tag} 对应的实验数据")

    experiment_name = experiment_data["experiment_name"]
    start_time = experiment_data["phase_start_time"]
    end_time = experiment_data["phase_end_time"]

    start_date = datetime.strptime(start_time.strftime("%Y-%m-%d"), "%Y-%m-%d")
    end_date = datetime.strptime(end_time.strftime("%Y-%m-%d"), "%Y-%m-%d")
    delta_days = (end_date - start_date).days

    engine = get_db_connection()
    table_name = f"tbl_report_avg_bot_view_{tag}"

    drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
    create_table_query = f"""
    CREATE TABLE {table_name} (
        event_date VARCHAR(255),
        variation_id VARCHAR(255),
        total_click BIGINT,
        total_user BIGINT,
        avg_bot_clicked DOUBLE,
        new_user_total_click BIGINT,
        new_user_total_user BIGINT,
        new_user_avg_bot_clicked DOUBLE
    );
    """

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(drop_table_query))
        conn.execute(text(create_table_query))
        print(f"✅ 表 {table_name} 已创建。")

        for d in range(1, delta_days):
            current_date = (start_date + timedelta(days=d)).strftime("%Y-%m-%d")
            print(f"👉 正在插入日期：{current_date}")

            query = f"""
            INSERT INTO {table_name} 
              (event_date, variation_id, total_click, total_user, avg_bot_clicked,
               new_user_total_click, new_user_total_user, new_user_avg_bot_clicked)
            WITH dedup_assignment AS (
                SELECT user_id, event_date, variation_id
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY user_id, event_date, experiment_id
                            ORDER BY timestamp_assigned
                        ) AS rn
                    FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                    WHERE experiment_id = '{experiment_name}'
                ) t
                WHERE rn = 1
            ),
            user_bot_cnt AS (
                SELECT
                    v.event_date,
                    a.variation_id,
                    v.user_id,
                    COUNT(DISTINCT v.bot_id) AS bot_cnt,
                    CASE WHEN n.user_id IS NOT NULL THEN 1 ELSE 0 END AS is_new_user
                FROM flow_event_info.tbl_app_event_bot_view v
                JOIN dedup_assignment a ON v.user_id = a.user_id AND v.event_date = a.event_date
                LEFT JOIN flow_wide_info.tbl_wide_user_first_visit_app_info n
                    ON v.user_id = n.user_id AND DATE(n.first_visit_date) = v.event_date
                WHERE v.event_date = '{current_date}'
                GROUP BY v.event_date, a.variation_id, v.user_id, is_new_user
            )
            SELECT
                '{current_date}' AS event_date,
                variation_id,
                SUM(bot_cnt) as total_click,
                COUNT(DISTINCT user_id) AS total_user,
                ROUND(SUM(bot_cnt)*1.0/COUNT(DISTINCT user_id), 4) AS avg_bot_clicked,
                SUM(CASE WHEN is_new_user=1 THEN bot_cnt ELSE 0 END) AS new_user_total_click,
                COUNT(DISTINCT CASE WHEN is_new_user=1 THEN user_id ELSE NULL END) AS new_user_total_user,
                ROUND(
                  SUM(CASE WHEN is_new_user=1 THEN bot_cnt ELSE 0 END)*1.0 /
                  NULLIF(COUNT(DISTINCT CASE WHEN is_new_user=1 THEN user_id ELSE NULL END),0), 4
                ) AS new_user_avg_bot_clicked
            FROM user_bot_cnt
            GROUP BY variation_id;
            """

            try:
                conn.execute(text(query))
            except Exception as e:
                print(f"❌ 插入 {current_date} 失败：{e}")
                print(f"🔍 SQL:\n{query}")

    result_df = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY event_date, variation_id;", engine)
    result_df.fillna(0, inplace=True)
    print("🚀 人均bot点击分析预览：")
    print(result_df)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        tag = sys.argv[1]
    else:
        tag = "mobile"
        print(f"⚠️ 未指定实验标签，默认使用：{tag}")
    main(tag)
