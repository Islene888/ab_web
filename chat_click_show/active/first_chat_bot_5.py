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
    password = urllib.parse.quote_plus(os.environ.get('DB_PASSWORD'))
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
    table_name = f"tbl_report_first_chat_bot_per_user_{tag}"

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
            new_user AS (
                SELECT user_id
                FROM flow_wide_info.tbl_wide_user_first_visit_app_info
                WHERE DATE(first_visit_date) = '{current_date}'
            )
            SELECT
                '{current_date}' AS event_date,
                d.variation_id AS variation_id,
                COUNT(DISTINCT CONCAT(c.user_id, '_', c.prompt_id)) AS total_click,
                COUNT(DISTINCT c.user_id) AS total_user,
                ROUND(COUNT(DISTINCT CONCAT(c.user_id, '_', c.prompt_id)) * 1.0 / NULLIF(COUNT(DISTINCT c.user_id), 0), 4) AS avg_bot_clicked,
                -- 新用户相关
                COUNT(DISTINCT CASE WHEN n.user_id IS NOT NULL THEN CONCAT(c.user_id, '_', c.prompt_id) END) AS new_user_total_click,
                COUNT(DISTINCT CASE WHEN n.user_id IS NOT NULL THEN c.user_id END) AS new_user_total_user,
                ROUND(
                    COUNT(DISTINCT CASE WHEN n.user_id IS NOT NULL THEN CONCAT(c.user_id, '_', c.prompt_id) END) * 1.0 /
                    NULLIF(COUNT(DISTINCT CASE WHEN n.user_id IS NOT NULL THEN c.user_id END), 0)
                , 4) AS new_user_avg_bot_clicked
            FROM flow_event_info.tbl_app_event_chat_send c
            JOIN dedup_assignment d
              ON c.user_id = d.user_id AND c.event_date = d.event_date
            LEFT JOIN new_user n
              ON c.user_id = n.user_id
            WHERE c.event_date = '{current_date}'
            GROUP BY d.variation_id
            """
            try:
                conn.execute(text(query))
            except Exception as e:
                print(f"❌ 插入 {current_date} 失败：{e}")
                print(f"🔍 SQL:\n{query}")

    result_df = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY event_date, variation_id;", engine)
    result_df.fillna(0, inplace=True)
    print("🚀 人均新开聊bot个数分析预览：")
    print(result_df)
    # 如果需要导出为csv，可加如下代码
    # result_df.to_csv(f"{table_name}.csv", index=False, encoding='utf-8-sig')

if __name__ == "__main__":
    if len(sys.argv) > 1:
        tag = sys.argv[1]
    else:
        tag = "mobile"
        print(f"⚠️ 未指定实验标签，默认使用：{tag}")
    main(tag)
