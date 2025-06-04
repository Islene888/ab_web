import urllib.parse
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta


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

# 2. 插入指定日期的数据
def insert_home_explore_for_date(engine, target_date: str):
    with engine.connect() as conn:
        query = f"""
        INSERT INTO flow_wide_info.tbl_wide_daily_user_home_bot_info
        WITH show_info AS (
            SELECT user_id, COUNT(*) AS shows
            FROM flow_event_info.tbl_app_event_show_prompt_card
            WHERE event_date = '{target_date}'
              AND current_page = 'home'
              AND tab_name = 'Explore'
            GROUP BY user_id
        ),
        click_info AS (
            SELECT user_id, COUNT(*) AS clicks
            FROM flow_event_info.tbl_app_event_bot_view
            WHERE event_date = '{target_date}'
              AND source = 'tag:Explore'
            GROUP BY user_id
        ),
        chat_info AS (
            SELECT user_id, COUNT(*) AS chats
            FROM flow_event_info.tbl_app_event_chat_send
            WHERE event_date = '{target_date}'
              AND source = 'tag:Explore'
            GROUP BY user_id
        )
        SELECT
            /*+ SET_VAR (query_timeout = 30000) */
            '{target_date}' AS event_date,
            p.`"id"` AS user_id,
            s.shows AS shows,
            cl.clicks AS clicks,
            ch.chats AS chats
        FROM flow_rds_prod.tbl_wide_rds_user p
        LEFT JOIN show_info s ON p.`"id"` = s.user_id
        LEFT JOIN click_info cl ON p.`"id"` = cl.user_id
        LEFT JOIN chat_info ch ON p.`"id"` = ch.user_id;
        """

        try:
            conn.execute(text(query))
            print(f"✅ 插入成功：{target_date}")
        except Exception as e:
            print(f"❌ 插入失败：{target_date} - {e}")

# 3. 主程序（支持日期范围）
def main(start_date_str: str, end_date_str: str):
    engine = get_engine()
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date   = datetime.strptime(end_date_str, "%Y-%m-%d")

    days = (end_date - start_date).days + 1
    for i in range(days):
        current_date = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
        insert_home_explore_for_date(engine, current_date)

# 4. 可通过命令行参数运行，或直接在文件中指定
if __name__ == "__main__":
    # 示例：可替换为动态参数或 sys.argv
    start_date = "2025-05-14"
    end_date = "2025-05-26"
    print(f"🚀 开始插入 Explore 首页行为数据：{start_date} 至 {end_date}")
    main(start_date, end_date)
