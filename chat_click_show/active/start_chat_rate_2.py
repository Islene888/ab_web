import os
import sys
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

warnings.filterwarnings("ignore", category=FutureWarning)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

import logging
import os
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
    table_name = f"tbl_report_chat_start_rate_{tag}"

    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            event_date VARCHAR(255), -- Consider changing to DATE type as discussed previously
            variation VARCHAR(255),
            clicked_bots INT,
            chat_bots INT,
            chat_start_rate DOUBLE,
            new_clicked_bots INT,
            new_chat_bots INT,
            new_chat_start_rate DOUBLE,
            clicked_users INT,  -- ⚡️ 👈 Add this line (or BIGINT if counts are very large)
            started_users INT,
            user_chat_start_rate DOUBLE,
            new_user_chat_start_rate DOUBLE,
            experiment_name VARCHAR(255)
        );
        """

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        logging.info(f"✅ 表 {table_name} 准备就绪。")

        conn.execute(text(f"TRUNCATE TABLE {table_name};"))
        logging.warning(f"⚠️ 表 {table_name} 已清空旧数据。")

        start_date = datetime.strptime(start_day_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_day_str, "%Y-%m-%d")
        delta_days = (end_date - start_date).days

        for d in range(1, delta_days):
            current_date = (start_date + timedelta(days=d)).strftime("%Y-%m-%d")


            insert_sql = f"""
            INSERT INTO tbl_report_chat_start_rate_{tag}(
                event_date, variation, clicked_bots, chat_bots, chat_start_rate,
                new_clicked_bots, new_chat_bots, new_chat_start_rate, clicked_users, -- Here!
                started_users, user_chat_start_rate, new_user_chat_start_rate, experiment_name
            )
            WITH dedup_assignment AS (
                SELECT user_id, event_date, variation_id
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY user_id, event_date, experiment_id
                               ORDER BY variation_id
                           ) AS rn
                    FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                    WHERE experiment_id = '{experiment_name}'
                ) t
                WHERE rn = 1
            ),
            base_view AS (
                SELECT user_id, bot_id, event_date
                FROM flow_event_info.tbl_app_event_bot_view
                WHERE event_date = '{current_date}'
            ),
            base_chat AS (
                SELECT user_id, prompt_id, event_date
                FROM flow_event_info.tbl_app_event_chat_send
                WHERE event_date = '{current_date}'
            ),
            new_users AS (
                SELECT user_id
                FROM flow_wide_info.tbl_wide_user_first_visit_app_info
                WHERE DATE(first_visit_date) = '{current_date}'
            ),
            joined AS (
                SELECT
                    a.variation_id,
                    v.bot_id AS clicked_bot,
                    cs.prompt_id AS chat_bot,
                    v.user_id,
                    CASE WHEN nu.user_id IS NOT NULL THEN 1 ELSE 0 END AS is_new_user
                FROM dedup_assignment a
                JOIN base_view v
                    ON a.user_id = v.user_id AND a.event_date = v.event_date
                LEFT JOIN base_chat cs
                    ON v.user_id = cs.user_id AND v.bot_id = cs.prompt_id AND v.event_date = cs.event_date
                LEFT JOIN new_users nu
                    ON v.user_id = nu.user_id
            )
            SELECT
                '{current_date}' AS event_date,
                variation_id,
                COUNT(DISTINCT clicked_bot) AS clicked_bots,
                COUNT(DISTINCT chat_bot) AS chat_bots,
                CASE WHEN COUNT(DISTINCT clicked_bot) = 0 THEN 0
                     ELSE ROUND(COUNT(DISTINCT chat_bot) * 1.0 / COUNT(DISTINCT clicked_bot), 4)
                END AS chat_start_rate,
                COUNT(DISTINCT CASE WHEN is_new_user = 1 THEN clicked_bot END) AS new_clicked_bots,
                COUNT(DISTINCT CASE WHEN is_new_user = 1 THEN chat_bot END) AS new_chat_bots,
                CASE WHEN COUNT(DISTINCT CASE WHEN is_new_user = 1 THEN clicked_bot END) = 0 THEN 0
                     ELSE ROUND(
                         COUNT(DISTINCT CASE WHEN is_new_user = 1 THEN chat_bot END) * 1.0 /
                         COUNT(DISTINCT CASE WHEN is_new_user = 1 THEN clicked_bot END), 4)
                END AS new_chat_start_rate,
                COUNT(DISTINCT user_id) AS clicked_users,
                COUNT(DISTINCT CASE WHEN clicked_bot = chat_bot AND chat_bot IS NOT NULL THEN user_id END) AS started_users,
                CASE WHEN COUNT(DISTINCT user_id) = 0 THEN 0
                     ELSE ROUND(
                         COUNT(DISTINCT CASE WHEN clicked_bot = chat_bot AND chat_bot IS NOT NULL THEN user_id END) * 1.0 /
                         COUNT(DISTINCT user_id)
                     , 4)
                END AS user_chat_start_rate,
                CASE WHEN COUNT(DISTINCT CASE WHEN is_new_user = 1 THEN user_id END) = 0 THEN 0
                     ELSE ROUND(
                         COUNT(DISTINCT CASE WHEN is_new_user = 1 AND clicked_bot = chat_bot AND chat_bot IS NOT NULL THEN user_id END) * 1.0 /
                         COUNT(DISTINCT CASE WHEN is_new_user = 1 THEN user_id END)
                     , 4)
                END AS new_user_chat_start_rate,
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
        tag = "chat_0519"
        print(f"⚠️ 未指定实验标签，默认使用：{tag}")
    main(tag)
