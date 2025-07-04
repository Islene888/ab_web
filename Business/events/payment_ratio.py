import logging
import os
import urllib.parse
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from datetime import timedelta

from growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
load_dotenv()

def get_db_connection():
    password = urllib.parse.quote_plus(os.environ['DB_PASSWORD'])
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    logging.info("✅ 数据库连接已建立。")
    return engine

def insert_payment_ratio_data(tag, event_date, experiment_name, engine, table_name, truncate=False):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        variation_id VARCHAR(255),
        event_date DATE,
        paying_users INT,
        revenue DOUBLE,
        LTV7 DOUBLE,
        LTV_experiment DOUBLE,
        total_users INT,
        purchase_rate DOUBLE,
        experiment_tag VARCHAR(255)
    );
    """
    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        if truncate:
            conn.execute(text(f"TRUNCATE TABLE {table_name};"))
            print(f"✅ 目标表 {table_name} 已创建并清空数据。")
        insert_query = f"""
        INSERT INTO {table_name} (variation_id, event_date, paying_users, revenue, LTV7, LTV_experiment, total_users, purchase_rate, experiment_tag)
        WITH 
        exp AS (
          SELECT user_id, variation_id, event_date
          FROM (
            SELECT
              user_id,
              variation_id,
              event_date,
              ROW_NUMBER() OVER (PARTITION BY user_id, event_date ORDER BY event_date DESC) AS rn
            FROM flow_wide_info.tbl_wide_experiment_assignment_hi
            WHERE experiment_id = '{experiment_name}'
              AND event_date = '{event_date}'
          ) t
          WHERE rn = 1
        ),
        first_active AS (
          SELECT user_id, variation_id, MIN(event_date) AS first_active_date
          FROM exp
          GROUP BY user_id, variation_id
        ),
        act_raw AS (
          SELECT DISTINCT
            e.user_id,
            e.variation_id,
            p.event_date
          FROM exp e
          JOIN flow_event_info.tbl_app_event_all_purchase p
            ON e.user_id = p.user_id
           AND p.event_date = '{event_date}'
        ),
        purchase AS (
          SELECT
            e.user_id,
            e.variation_id,
            p.event_date,
            p.revenue,
            DATEDIFF(p.event_date, f.first_active_date) AS day_diff
          FROM exp e
          JOIN first_active f ON e.user_id = f.user_id AND e.variation_id = f.variation_id
          JOIN flow_event_info.tbl_app_event_all_purchase p
            ON e.user_id = p.user_id
           AND p.type IN ('subscription', 'currency')
           AND p.event_date = '{event_date}'
        ),
        final_purchase AS (
          SELECT DISTINCT user_id, variation_id, event_date, revenue, day_diff
          FROM purchase
        ),
        final_activity AS (
          SELECT DISTINCT user_id, variation_id, event_date
          FROM act_raw
        )
        SELECT 
          a.variation_id,
          a.event_date,
          COUNT(DISTINCT p.user_id) AS paying_users,
          SUM(IFNULL(p.revenue, 0)) AS revenue,
          ROUND(SUM(CASE WHEN p.day_diff <= 7 THEN p.revenue ELSE 0 END) / NULLIF(COUNT(DISTINCT p.user_id), 0), 4) AS LTV7,
          ROUND(SUM(IFNULL(p.revenue, 0)) / NULLIF(COUNT(DISTINCT p.user_id), 0), 4) AS LTV_experiment,
          COUNT(DISTINCT a.user_id) AS total_users,
          ROUND(COUNT(DISTINCT p.user_id) / NULLIF(COUNT(DISTINCT a.user_id), 0), 4) AS purchase_rate,
          '{tag}' AS experiment_tag
        FROM final_activity a
        LEFT JOIN final_purchase p
          ON a.user_id = p.user_id AND a.variation_id = p.variation_id AND a.event_date = p.event_date
        GROUP BY a.variation_id, a.event_date;
        """
        conn.execute(text(insert_query))
        print(f"✅ 数据已插入：{event_date}")

def daterange(start_date, end_date):
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)

def main(tag):
    print("🚀 主流程开始执行。")
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"⚠️ 没有找到符合标签 '{tag}' 的实验数据！")
        return

    experiment_name = experiment_data['experiment_name']
    start_time = experiment_data['phase_start_time'].date()  # datetime.date
    end_time = experiment_data['phase_end_time'].date()
    table_name = f"tbl_report_payment_ratio_{tag}"

    engine = get_db_connection()
    truncate = True
    for d in daterange(start_time + timedelta(days=1), end_time):
        insert_payment_ratio_data(
            tag=tag,
            event_date=d.strftime("%Y-%m-%d"),
            experiment_name=experiment_name,
            engine=engine,
            table_name=table_name,
            truncate=truncate
        )
        truncate = False  # 只在首次循环清空
    print("🚀 所有日期数据写入完毕。")

if __name__ == "__main__":
    main("mobile")
