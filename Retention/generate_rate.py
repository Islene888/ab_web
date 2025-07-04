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

    experiment_name = experiment_data["experiment_name"]  # 用于 SQL 条件
    start_time = experiment_data["phase_start_time"]
    end_time = experiment_data["phase_end_time"]

    start_date = datetime.strptime(start_time.strftime("%Y-%m-%d"), "%Y-%m-%d")
    end_date = datetime.strptime(end_time.strftime("%Y-%m-%d"), "%Y-%m-%d")
    delta_days = (end_date - start_date).days

    engine = get_db_connection()
    table_name = f"tbl_report_generate_image_use_rate_{tag}"

    drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
    create_table_query = f"""
    CREATE TABLE {table_name} (
        event_day VARCHAR(20),
        variation_id VARCHAR(64),
        test_chat_users BIGINT,
        generate_user BIGINT,
        generate_image_use_rate DOUBLE
    );
    """

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(drop_table_query))
        conn.execute(text(create_table_query))
        print(f"✅ 表 {table_name} 已创建。")

        for d in range(0, delta_days):
            current_date = (start_date + timedelta(days=d)).strftime("%Y-%m-%d")
            print(f"👉 正在插入日期：{current_date}")

            # 这里的 experiment_id 直接用 experiment_name 变量
            query = f"""
            INSERT INTO {table_name}
            (event_day, variation_id, test_chat_users, generate_user, generate_image_use_rate)
            WITH test_users AS (
                SELECT
                    chat.event_date AS event_day,
                    expr.variation_id,
                    COUNT(DISTINCT chat.user_id) AS test_chat_users
                FROM flow_event_info.tbl_app_event_chat_send AS chat
                INNER JOIN flow_wide_info.tbl_wide_experiment_assignment_hi AS expr
                    ON chat.event_date = expr.event_date
                   AND chat.user_id   = expr.user_id
                WHERE chat.event_date = '{current_date}'
                  AND expr.experiment_id = '{experiment_name}'
                GROUP BY event_day, expr.variation_id
            ),
            generate_users AS (
                SELECT
                    gen.event_date AS event_day,
                    expr.variation_id,
                    COUNT(DISTINCT gen.user_id) AS generate_user
                FROM flow_event_info.tbl_app_event_chat_image_generate AS gen
                INNER JOIN flow_wide_info.tbl_wide_experiment_assignment_hi AS expr
                    ON gen.event_date = expr.event_date
                   AND gen.user_id   = expr.user_id
                WHERE expr.experiment_id = '{experiment_name}'
                  AND gen.event_date = '{current_date}'
                GROUP BY event_day, expr.variation_id
            )
            SELECT
                t.event_day,
                t.variation_id,
                t.test_chat_users,
                COALESCE(g.generate_user, 0) AS generate_user,
                ROUND(COALESCE(g.generate_user, 0) / t.test_chat_users, 3) AS generate_image_use_rate
            FROM test_users t
            LEFT JOIN generate_users g
                ON t.event_day = g.event_day
               AND t.variation_id = g.variation_id;
            """

            try:
                conn.execute(text(query))
            except Exception as e:
                print(f"❌ 插入 {current_date} 失败：{e}")
                print(f"🔍 SQL:\n{query}")

    # 结果展示
    result_df = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY event_day, variation_id;", engine)
    result_df.fillna(0, inplace=True)
    print("🚀 chat-generate-image每日分析预览：")
    print(result_df)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        tag = sys.argv[1]
    else:
        tag = "mobile_new"
        print(f"⚠️ 未指定实验标签，默认使用：{tag}")
    main(tag)
