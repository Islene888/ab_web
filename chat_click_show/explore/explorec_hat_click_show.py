import sys
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
from datetime import datetime, timedelta

from state3.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

warnings.filterwarnings("ignore", category=FutureWarning)


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
    print(f"🚀 开始插入点击率和开聊率，标签：{tag}")

    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"⚠️ 没有找到符合标签 '{tag}' 的实验数据！")
        return

    experiment_name = experiment_data['experiment_name']
    start_time = experiment_data['phase_start_time']
    end_time = experiment_data['phase_end_time']

    start_date = datetime.strptime(start_time.strftime("%Y-%m-%d"), "%Y-%m-%d")
    end_date = datetime.strptime(end_time.strftime("%Y-%m-%d"), "%Y-%m-%d")
    delta_days = (end_date - start_date).days

    engine = get_db_connection()
    table_name = f"tbl_report_click_chat_rate_explore_{tag}"

    drop_table_sql = f"DROP TABLE IF EXISTS {table_name};"
    create_table_sql = f"""
    CREATE TABLE {table_name} (
        event_date VARCHAR(10),
        variation VARCHAR(255),
        show_users BIGINT,
        click_users BIGINT,
        chat_users BIGINT,
        click_rate DOUBLE,
        chat_start_rate DOUBLE,
        new_show_users BIGINT,
        new_click_users BIGINT,
        new_chat_users BIGINT,
        new_click_rate DOUBLE,
        new_chat_start_rate DOUBLE,
        experiment_name VARCHAR(255)
    );
    """

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(drop_table_sql))
        conn.execute(text(create_table_sql))
        print(f"✅ 表 {table_name} 已创建。")

        for d in range(1, delta_days):  # 排除首尾
            current_date = (start_date + timedelta(days=d)).strftime("%Y-%m-%d")
            print(f"👉 插入日期：{current_date}")
            insert_sql = f"""
            INSERT INTO {table_name} (
                event_date, variation, show_users, click_users, chat_users,
                click_rate, chat_start_rate,
                new_show_users, new_click_users, new_chat_users,
                new_click_rate, new_chat_start_rate,
                experiment_name
            )
         WITH base_users AS (
    SELECT
        t.user_id,
        t.event_date,
        h.variation_id,
        CASE WHEN fv.user_id IS NOT NULL THEN 1 ELSE 0 END AS is_new_user,
        t.shows,  -- 曝光次数
        t.clicks, -- 点击次数
        t.chats   -- 开聊次数
    FROM flow_wide_info.tbl_wide_daily_user_home_bot_info AS t
    INNER JOIN flow_wide_info.tbl_wide_experiment_assignment_hi AS h
        ON t.user_id = h.user_id
        AND DATE(h.event_date) = '{current_date}'    -- ✅ 改成当天打标签
    LEFT JOIN flow_wide_info.tbl_wide_user_first_visit_app_info AS fv
        ON t.user_id = fv.user_id
        AND DATE(fv.first_visit_date) = '{current_date}'
    WHERE h.experiment_id = '{experiment_name}'
      AND t.event_date = '{current_date}'
)
SELECT
    '{current_date}' AS event_date,
    variation_id,

    SUM(shows) AS total_shows,
    SUM(clicks) AS total_clicks,
    SUM(chats) AS total_chats,

    ROUND(SUM(clicks) * 1.0 / NULLIF(SUM(shows), 0), 4) AS click_rate,
    ROUND(SUM(chats) * 1.0 / NULLIF(SUM(clicks), 0), 4) AS chat_start_rate,

    SUM(CASE WHEN is_new_user = 1 THEN shows ELSE 0 END) AS new_total_shows,
    SUM(CASE WHEN is_new_user = 1 THEN clicks ELSE 0 END) AS new_total_clicks,
    SUM(CASE WHEN is_new_user = 1 THEN chats ELSE 0 END) AS new_total_chats,

    ROUND(
        SUM(CASE WHEN is_new_user = 1 THEN clicks ELSE 0 END) * 1.0 /
        NULLIF(SUM(CASE WHEN is_new_user = 1 THEN shows ELSE 0 END), 0), 4
    ) AS new_click_rate,

    ROUND(
        SUM(CASE WHEN is_new_user = 1 THEN chats ELSE 0 END) * 1.0 /
        NULLIF(SUM(CASE WHEN is_new_user = 1 THEN clicks ELSE 0 END), 0), 4
    ) AS new_chat_start_rate,

    '{experiment_name}' AS experiment_name

FROM base_users
GROUP BY variation_id;

            """
            try:
                conn.execute(text(insert_sql))
            except Exception as e:
                print(f"❌ 插入 {current_date} 失败：{e}")
                print(insert_sql)

        print(f"✅ 所有数据已插入表 {table_name}。")

    # 预览
    df = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY event_date, variation;", engine)
    df.fillna(0, inplace=True)
    print("🚀 插入完成，数据预览：")
    print(df)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        tag = sys.argv[1]
    else:
        tag = "onboarding_new_tag"
        print(f"⚠️ 未指定实验标签，默认使用：{tag}")
    main(tag)
