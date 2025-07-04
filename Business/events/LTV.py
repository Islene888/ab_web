import sys
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import logging
import os
from dotenv import load_dotenv

from growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

load_dotenv()

def get_db_connection():
    password = urllib.parse.quote_plus(os.environ['DB_PASSWORD'])
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    logging.info("✅ 数据库连接已建立。")
    return engine

def main(tag: str):
    table_name = f"tbl_report_ltv_{tag}"
    drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
    create_table_query = f'''
    CREATE TABLE {table_name} (
        event_date DATE,
        variation_id VARCHAR(255),
        register_users BIGINT,
        revenue_7d DOUBLE,
        revenue_cycle DOUBLE,
        ltv_7d DOUBLE,
        ltv_cycle DOUBLE
    );
    '''

    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        raise ValueError(f"⚠️ 没有找到实验标签 {tag} 对应的实验数据")

    experiment_name = experiment_data["experiment_name"]
    start_time = experiment_data["phase_start_time"]
    end_time = experiment_data["phase_end_time"]

    start_date = datetime.strptime(start_time.strftime("%Y-%m-%d"), "%Y-%m-%d")
    end_date = datetime.strptime(end_time.strftime("%Y-%m-%d"), "%Y-%m-%d")
    delta_days = (end_date - start_date).days + 1
    cycle_days = delta_days - 1  # 包含头不含尾，eg: 6.1 ~ 6.10 => 9

    # SQL 查询模板
    query_template = """
    WITH experiment_users AS (
      SELECT
        user_id,
        CAST(variation_id AS CHAR) AS variation_id
      FROM (
        SELECT
          user_id,
          variation_id,
          ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp_assigned ASC) AS rn
        FROM flow_wide_info.tbl_wide_experiment_assignment_hi
        WHERE experiment_id = '{experiment_name}'
      ) t
      WHERE rn = 1
    ),
    revenue_7d AS (
      SELECT
        bu.user_id,
        bu.first_active_date AS event_date,
        eu.variation_id,
        SUM(COALESCE(udr.ad_revenue, 0) + COALESCE(udr.subscribe_revenue, 0) + COALESCE(udr.flux_revenue, 0)) AS revenue_7d
      FROM flow_event_info.view_business_user bu
      LEFT JOIN experiment_users eu ON bu.user_id = eu.user_id
      LEFT JOIN flow_event_info.view_user_daily_revenue udr
        ON bu.user_id = udr.user_id
        AND udr.event_date BETWEEN bu.first_active_date AND DATE_ADD(bu.first_active_date, INTERVAL 7 DAY)
      WHERE bu.first_active_date = '{current_date}'
        GROUP BY bu.user_id, bu.first_active_date, eu.variation_id
    ),
    revenue_cycle AS (
      SELECT
        bu.user_id,
        bu.first_active_date AS event_date,
        eu.variation_id,
        SUM(COALESCE(udr.ad_revenue, 0) + COALESCE(udr.subscribe_revenue, 0) + COALESCE(udr.flux_revenue, 0)) AS revenue_cycle
      FROM flow_event_info.view_business_user bu
      LEFT JOIN experiment_users eu ON bu.user_id = eu.user_id
      LEFT JOIN flow_event_info.view_user_daily_revenue udr
        ON bu.user_id = udr.user_id
        AND udr.event_date BETWEEN bu.first_active_date AND DATE_ADD(bu.first_active_date, INTERVAL {cycle_days} DAY)
      WHERE bu.first_active_date = '{current_date}'
        GROUP BY bu.user_id, bu.first_active_date, eu.variation_id
    )
    SELECT
      r7.event_date,
      r7.variation_id,
      COUNT(DISTINCT r7.user_id) AS register_users,
      ROUND(SUM(r7.revenue_7d), 2) AS revenue_7d,
      ROUND(SUM(rc.revenue_cycle), 2) AS revenue_cycle,
      ROUND(SUM(r7.revenue_7d) / NULLIF(COUNT(DISTINCT r7.user_id), 0), 4) AS ltv_7d,
      ROUND(SUM(rc.revenue_cycle) / NULLIF(COUNT(DISTINCT r7.user_id), 0), 4) AS ltv_cycle
    FROM revenue_7d r7
    JOIN revenue_cycle rc
      ON r7.user_id = rc.user_id
      AND r7.event_date = rc.event_date
      AND r7.variation_id = rc.variation_id
    GROUP BY r7.event_date, r7.variation_id;
    """

    engine = get_db_connection()
    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(drop_table_query))
        conn.execute(text(create_table_query))
        print(f"✅ 表 {table_name} 已创建。")

        for d in range(1,delta_days):
            current_date = (start_date + timedelta(days=d)).strftime("%Y-%m-%d")
            print(f"👉 正在插入日期：{current_date}")

            query = query_template.format(
                experiment_name=experiment_name,
                current_date=current_date,
                cycle_days=cycle_days
            )
            insert_sql = f"INSERT INTO {table_name} {query}"
            try:
                conn.execute(text(insert_sql))
            except Exception as e:
                print(f"❌ 插入 {current_date} 失败：{e}")
                print(f"🔍 SQL:\n{insert_sql}")

    # 查询汇总结果
    result_df = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY event_date, variation_id;", engine)
    print("🚀 LTV 日表分析预览：")
    print(result_df)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        tag = sys.argv[1]
    else:
        tag = "mobile"
        print(f"⚠️ 未指定实验标签，默认使用：{tag}")
    main(tag)
