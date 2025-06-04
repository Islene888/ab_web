import sys
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
from datetime import datetime, timedelta

from growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

warnings.filterwarnings("ignore", category=FutureWarning)


# ========== 数据库连接 ==========

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

# ========== 主程序 ==========
def main(tag):
    print(f"🚀 开始插入曝光数据，标签：{tag}")

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
    table_name = f"tbl_report_user_show_summary_{tag}"

    # 建表（如果表不存在的话）
    drop_table_sql = f"DROP TABLE IF EXISTS {table_name};"
    create_table_sql = f"""
    CREATE TABLE {table_name} (
        event_date STRING,
        variation_id STRING,
        total_shows BIGINT,
        unique_users BIGINT,
        avg_shows_per_user DOUBLE,
        new_total_shows BIGINT,
        new_unique_users BIGINT,
        new_avg_shows_per_user DOUBLE
    );
    """

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(drop_table_sql))
        conn.execute(text(create_table_sql))
        print(f"✅ 表 {table_name} 已创建。")

        for d in range(1, delta_days):  # 排除首尾
            current_date = (start_date + timedelta(days=d)).strftime("%Y-%m-%d")
            print(f"👉 正在处理日期：{current_date}")

            query = f"""
WITH experiment_assignment_dedup AS (
    SELECT *
    FROM (
        SELECT
            user_id,
            experiment_id,
            variation_id,
            event_date,
            ROW_NUMBER() OVER (PARTITION BY user_id, experiment_id ORDER BY event_date ASC) AS rn
        FROM flow_wide_info.tbl_wide_experiment_assignment_hi
        WHERE experiment_id = '{experiment_name}'
    ) t
    WHERE t.rn = 1
),
first_visit_dedup AS (
    SELECT
        user_id,
        MIN(first_visit_date) AS first_visit_date
    FROM flow_wide_info.tbl_wide_user_first_visit_app_info
    GROUP BY user_id
),
base_users AS (
    SELECT
        t.user_id,
        d.variation_id,
        COUNT(DISTINCT t.event_id) AS shows,
        MAX(CASE WHEN fv.user_id IS NOT NULL AND DATE(fv.first_visit_date) = '{current_date}' THEN 1 ELSE 0 END) AS is_new_user
    FROM flow_event_info.tbl_app_event_show_prompt_card t
    INNER JOIN experiment_assignment_dedup d
        ON t.user_id = d.user_id
    LEFT JOIN first_visit_dedup fv
        ON t.user_id = fv.user_id
    WHERE t.event_date = '{current_date}'
      AND t.current_page = 'home'
      AND t.tab_name = 'Explore'
      AND t.user_id IS NOT NULL
    GROUP BY t.user_id, d.variation_id
)
SELECT
    '{current_date}' AS event_date,
    CAST(variation_id AS STRING) AS variation_id,
    SUM(shows) AS total_shows,
    COUNT(DISTINCT user_id) AS unique_users,
    ROUND(SUM(shows) * 1.0 / NULLIF(COUNT(DISTINCT user_id), 0), 4) AS avg_shows_per_user,
    SUM(CASE WHEN is_new_user = 1 THEN shows ELSE 0 END) AS new_total_shows,
    COUNT(DISTINCT CASE WHEN is_new_user = 1 THEN user_id ELSE NULL END) AS new_unique_users,
    ROUND(SUM(CASE WHEN is_new_user = 1 THEN shows ELSE 0 END) * 1.0 / NULLIF(COUNT(DISTINCT CASE WHEN is_new_user = 1 THEN user_id ELSE NULL END), 0), 4) AS new_avg_shows_per_user
FROM base_users
GROUP BY variation_id
ORDER BY variation_id;
            """

            insert_sql = f"""
                INSERT INTO {table_name}
                {query}
            """

            try:
                conn.execute(text(insert_sql))
                print(f"✅ 插入 {current_date} 成功。")
            except Exception as e:
                print(f"❌ 插入 {current_date} 失败：{e}")
                print(insert_sql)

        print(f"🎯 所有数据已插入表 {table_name}。")

    # 查询预览
    df = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY event_date, variation_id;", engine)
    df.fillna(0, inplace=True)
    print("🚀 插入完成，数据预览：")
    print(df)


# ========== 程序入口 ==========
if __name__ == "__main__":
    if len(sys.argv) > 1:
        tag = sys.argv[1]
    else:
        tag = "chat_0519"
        print(f"⚠️ 未指定实验标签，默认使用：{tag}")
    main(tag)
