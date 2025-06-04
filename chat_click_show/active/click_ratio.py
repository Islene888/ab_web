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
    table_name = f"tbl_report_view_ratio_{tag}"

    drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
    create_table_query = f"""
    CREATE TABLE {table_name} (
        event_date VARCHAR(255),
        variation VARCHAR(255),
        showed_events BIGINT,
        clicked_events BIGINT,
        click_ratio DOUBLE,
        new_showed_events BIGINT,
        new_clicked_events BIGINT,
        new_user_click_ratio DOUBLE,
        experiment_name VARCHAR(255)
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
            INSERT INTO {table_name} (
                event_date, variation, showed_events, clicked_events, click_ratio,
                new_showed_events, new_clicked_events, new_user_click_ratio,
                experiment_name
            )
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
            base_show AS (
                SELECT user_id, event_date, COUNT(*) AS show_times
                FROM flow_event_info.tbl_app_event_show_prompt_card
                WHERE event_date = '{current_date}'
                GROUP BY user_id, event_date
            ),
            base_view AS (
                SELECT user_id, event_date, COUNT(*) AS click_times
                FROM flow_event_info.tbl_app_event_bot_view
                WHERE event_date = '{current_date}'
                GROUP BY user_id, event_date
            ),
            new_users AS (
                SELECT user_id
                FROM flow_wide_info.tbl_wide_user_first_visit_app_info
                WHERE DATE(first_visit_date) = '{current_date}'
            ),
            joined_data AS (
                SELECT
                    a.variation_id,
                    s.user_id AS show_user_id,
                    s.show_times,
                    COALESCE(v.click_times, 0) AS click_times,
                    CASE WHEN n.user_id IS NOT NULL THEN 1 ELSE 0 END AS is_new_user
                FROM dedup_assignment a
                LEFT JOIN base_show s
                    ON a.user_id = s.user_id AND a.event_date = s.event_date
                LEFT JOIN base_view v
                    ON s.user_id = v.user_id AND s.event_date = v.event_date
                LEFT JOIN new_users n
                    ON s.user_id = n.user_id
            )
            SELECT
                '{current_date}' AS event_date,
                variation_id,
                SUM(show_times) AS showed_events,
                SUM(click_times) AS clicked_events,
                ROUND(SUM(click_times) * 1.0 / NULLIF(SUM(show_times), 0), 4) AS click_ratio,
                SUM(CASE WHEN is_new_user = 1 THEN show_times ELSE 0 END) AS new_showed_events,
                SUM(CASE WHEN is_new_user = 1 THEN click_times ELSE 0 END) AS new_clicked_events,
                ROUND(
                    SUM(CASE WHEN is_new_user = 1 THEN click_times ELSE 0 END) * 1.0 /
                    NULLIF(SUM(CASE WHEN is_new_user = 1 THEN show_times ELSE 0 END), 0)
                , 4) AS new_user_click_ratio,
                '{experiment_name}' AS experiment_name
            FROM joined_data
            GROUP BY variation_id;
            """

            try:
                conn.execute(text(query))
            except Exception as e:
                print(f"❌ 插入 {current_date} 失败：{e}")
                print(f"🔍 SQL:\n{query}")

    result_df = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY event_date, variation;", engine)
    result_df.fillna(0, inplace=True)
    print("🚀 浏览行为分析预览：")
    print(result_df)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        tag = sys.argv[1]
    else:
        tag = "chat_0519"
        print(f"⚠️ 未指定实验标签，默认使用：{tag}")
    main(tag)
