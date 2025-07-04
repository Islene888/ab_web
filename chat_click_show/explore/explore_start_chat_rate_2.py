import sys
import urllib.parse
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import warnings
import logging
from datetime import datetime, timedelta
from growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

warnings.filterwarnings("ignore", category=FutureWarning)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


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

def main(tag):
    logging.info(f"🚀 开始获取实验数据，标签：{tag}")
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        logging.warning(f"⚠️ 没有找到符合标签 '{tag}' 的实验数据！")
        return None

    experiment_name = experiment_data["experiment_name"]
    start_time = experiment_data["phase_start_time"]
    end_time = experiment_data["phase_end_time"]

    start_day_str = start_time.strftime("%Y-%m-%d")
    end_day_str = end_time.strftime("%Y-%m-%d")
    engine = get_db_connection()
    table_name = f"tbl_report_chat_start_rate_explore_{tag}"


    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        event_date VARCHAR(255),
        variation VARCHAR(255),
        clicked_users INT,
        chat_users INT,
        chat_start_rate DOUBLE,
        new_clicked_users INT,
        new_chat_users INT,
        new_chat_start_rate DOUBLE,
        experiment_name VARCHAR(255)
    );
    """

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        logging.info(f"✅ 表 {table_name} 准备就绪。")

        conn.execute(text(f"TRUNCATE TABLE {table_name};"))
        logging.info(f"⚠️ 表 {table_name} 已清空。")
        logging.info(f"✅ 表 {table_name} 准备就绪。")

        start_date = datetime.strptime(start_day_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_day_str, "%Y-%m-%d")
        delta_days = (end_date - start_date).days

        for d in range(1, delta_days):
            current_date = (start_date + timedelta(days=d)).strftime("%Y-%m-%d")

            insert_sql = f"""
            INSERT INTO {table_name} (
                event_date, variation, clicked_users, chat_users, chat_start_rate,
                new_clicked_users, new_chat_users, new_chat_start_rate,
                experiment_name
            )
            WITH dedup_assignment AS (
                SELECT user_id, event_date, variation_id
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY user_id, event_date, experiment_id ORDER BY variation_id) AS rn
                    FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                    WHERE experiment_id = '{experiment_name}'
                ) t
                WHERE rn = 1
            ),
            base_view AS (
                SELECT DISTINCT user_id, event_date
                FROM flow_event_info.tbl_app_event_bot_view
                WHERE event_date = '{current_date}' AND source = 'tag:Explore'
            ),
            base_chat AS (
                SELECT DISTINCT user_id, event_date
                FROM flow_event_info.tbl_app_event_chat_send
                WHERE event_date = '{current_date}' AND source = 'tag:Explore'
            ),
            new_users AS (
                SELECT DISTINCT user_id
                FROM flow_wide_info.tbl_wide_user_first_visit_app_info
                WHERE DATE(first_visit_date) = '{current_date}'
            ),
            joined AS (
                SELECT
                    a.variation_id,
                    v.user_id,
                    CASE WHEN c.user_id IS NOT NULL THEN 1 ELSE 0 END AS has_chat,
                    CASE WHEN n.user_id IS NOT NULL THEN 1 ELSE 0 END AS is_new_user
                FROM dedup_assignment a
                JOIN base_view v
                    ON a.user_id = v.user_id AND a.event_date = v.event_date
                LEFT JOIN base_chat c
                    ON v.user_id = c.user_id AND v.event_date = c.event_date
                LEFT JOIN new_users n
                    ON v.user_id = n.user_id
            )
            SELECT
                '{current_date}' AS event_date,
                variation_id,
                COUNT(DISTINCT user_id) AS clicked_users,
                COUNT(DISTINCT CASE WHEN has_chat = 1 THEN user_id END) AS chat_users,
                CASE WHEN COUNT(DISTINCT user_id) = 0 THEN 0
                    ELSE ROUND(
                        COUNT(DISTINCT CASE WHEN has_chat = 1 THEN user_id END) * 1.0 / 
                        COUNT(DISTINCT user_id), 4)
                END AS chat_start_rate,
                COUNT(DISTINCT CASE WHEN is_new_user = 1 THEN user_id END) AS new_clicked_users,
                COUNT(DISTINCT CASE WHEN is_new_user = 1 AND has_chat = 1 THEN user_id END) AS new_chat_users,
                CASE WHEN COUNT(DISTINCT CASE WHEN is_new_user = 1 THEN user_id END) = 0 THEN 0
                    ELSE ROUND(
                        COUNT(DISTINCT CASE WHEN is_new_user = 1 AND has_chat = 1 THEN user_id END) * 1.0 /
                        COUNT(DISTINCT CASE WHEN is_new_user = 1 THEN user_id END), 4)
                END AS new_chat_start_rate,
                '{experiment_name}' AS experiment_name
            FROM joined
            GROUP BY variation_id;
            """
            logging.info(f"👉 正在插入日期：{current_date}")
            try:
                conn.execute(text(insert_sql))
            except Exception as e:
                logging.error(f"❌ 插入 {current_date} 失败：{e}")
                logging.debug(f"🔍 SQL:\n{insert_sql}")

        logging.info(f"✅ 所有开聊率数据已插入表 {table_name}。")

    result_df = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY event_date, variation;", engine)
    logging.info("🚀 开聊率预览：")
    logging.info(result_df)
    return table_name

if __name__ == "__main__":
    if len(sys.argv) > 1:
        tag = sys.argv[1]
    else:
        tag = "trans_pt"
        print(f"⚠️ 未指定实验标签，默认使用：{tag}")
    main(tag)
