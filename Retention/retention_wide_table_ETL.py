import urllib.parse
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
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



def insert_experiment_data_to_wide_table(tag):
    try:
        # 获取实验的详细信息
        experiment_data = get_experiment_details_by_tag(tag)
        if not experiment_data:
            print(f"没有找到符合标签 '{tag}' 的实验数据！")
            return

        experiment_name = experiment_data['experiment_name']
        start_time = experiment_data['phase_start_time']
        end_time = experiment_data['phase_end_time']
        variations = experiment_data['number_of_variations']
        control_group_key = experiment_data['control_group_key']

        # 时间数据格式化
        formatted_start_time = start_time.strftime('%Y-%m-%d')
        formatted_end_time = end_time.strftime('%Y-%m-%d')


        # 创建数据库连接
        engine = get_db_connection()

        # 动态构建表名（原表，用于分批数据插入及后续聚合覆盖）
        table_name = f"tbl_wide_user_retention_{tag}"  # 宽表表名
        report_table_name = f"tbl_report_user_retention_{tag}"  # 报告表表名

        # 创建宽表和报告表（如果不存在）
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            dt DATE,
            variation VARCHAR(255),
            new_users INT,
            d1 INT,
            d3 INT,
            d7 INT,
            d15 INT,
            total_assigned INT
        );
        """

        create_report_table_query = f"""
        CREATE TABLE IF NOT EXISTS {report_table_name} (
            dt DATE,
            day INT,
            variation VARCHAR(255),
            users INT,
            retained INT,
            retention_rate DOUBLE,
            ci_lower DOUBLE,
            ci_upper DOUBLE,
            control_rate DOUBLE,
            exp_rate DOUBLE,
            uplift DOUBLE,
            uplift_ci_lower DOUBLE,
            uplift_ci_upper DOUBLE,
            z_score DOUBLE,
            p_value DOUBLE,
            retention_rate_baseline DOUBLE
        );
        """
        # 创建宽表
        try:
            with engine.connect() as conn:
                conn.execute(text(create_table_query))
            print(f"✅ 宽表 {table_name} 已成功创建！")
        except SQLAlchemyError as e:
            print(f"🚨 宽表数据库表格创建失败: {e}")

        # 创建报告表
        try:
            with engine.connect() as conn:
                conn.execute(text(create_report_table_query))
            print(f"✅ 报告表 {report_table_name} 已成功创建！")
        except SQLAlchemyError as e:
            print(f"🚨 报告表数据库表格创建失败: {e}")

        # 清空宽表中原有数据（分批数据）
        try:
            with engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE {table_name};"))
            print(f"✅ 表 {table_name} 已成功清空原有数据！")
        except SQLAlchemyError as e:
            print(f"🚨 清空数据失败: {e}")

        # 使用 CRC32 函数对 user_id 转数字，利用 MOD 方法分批执行插入
        batch_count = 20  # 可根据数据量调整分批数
        for i in range(batch_count):
            insert_query = f"""            
              INSERT INTO {table_name} (dt, variation, new_users, d1, d3, d7, d15, total_assigned)
SELECT
    /*+ SET_VAR (query_timeout = 30000) */ 
    u.first_visit_date AS dt, 
    e.variation, 
    COUNT(DISTINCT u.user_id) AS new_users,
    COUNT(DISTINCT CASE WHEN DATEDIFF(a.active_date, u.first_visit_date) = 1 THEN a.user_id END) AS d1,
    COUNT(DISTINCT CASE WHEN DATEDIFF(a.active_date, u.first_visit_date) = 3 THEN a.user_id END) AS d3,
    COUNT(DISTINCT CASE WHEN DATEDIFF(a.active_date, u.first_visit_date) = 7 THEN a.user_id END) AS d7,
    COUNT(DISTINCT CASE WHEN DATEDIFF(a.active_date, u.first_visit_date) = 15 THEN a.user_id END) AS d15,
    MAX(COALESCE(ta.total_assigned, 0)) AS total_assigned
FROM (
    -- 严格新用户定义：筛选指定日期区间内首次访问的用户
    SELECT 
        user_id,
        DATE(first_visit_date) AS first_visit_date
    FROM flow_wide_info.tbl_wide_user_first_visit_app_info
    WHERE first_visit_date BETWEEN '{formatted_start_time}' AND '{formatted_end_time}'
) u
LEFT JOIN (
    -- 活跃用户行为表
    SELECT
        d.user_id,
        d.active_date
    FROM flow_wide_info.tbl_wide_active_user_app_info d
    WHERE
        d.active_date BETWEEN '{start_time}' AND '{end_time}'
        AND d.keep_alive_flag = 1
        AND d.user_id IS NOT NULL
        AND d.user_id != ''
    GROUP BY d.active_date, d.user_id
) a ON u.user_id = a.user_id
LEFT JOIN (
    -- ✅ 去重实验分组，只保留每个 user_id 最早的分配记录
    SELECT user_id, CAST(variation_id AS CHAR) AS variation
    FROM (
        SELECT
            user_id,
            variation_id,
            ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY timestamp_assigned ASC
            ) AS rn
        FROM flow_wide_info.tbl_wide_experiment_assignment_hi
        WHERE
            experiment_id = '{experiment_name}'
            AND timestamp_assigned BETWEEN '{start_time}' AND '{end_time}'
    ) t
    WHERE rn = 1
) e ON u.user_id = e.user_id
LEFT JOIN (
    -- ✅ 分组总分配人数（去重后）
    SELECT 
        DATE(timestamp_assigned) AS assign_date,
        CAST(variation_id AS CHAR) AS variation,
        COUNT(DISTINCT user_id) AS total_assigned
    FROM (
        SELECT user_id, variation_id, timestamp_assigned,
               ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp_assigned ASC) AS rn
        FROM flow_wide_info.tbl_wide_experiment_assignment_hi
        WHERE experiment_id = '{experiment_name}'
    ) t
    WHERE rn = 1
    GROUP BY DATE(timestamp_assigned), CAST(variation_id AS CHAR)
) ta ON ta.assign_date = u.first_visit_date AND ta.variation = e.variation
-- 排除未分组用户，并对 u.user_id 进行 CRC32 分批
WHERE e.variation IS NOT NULL
  AND MOD(CRC32(u.user_id), {batch_count}) = {i}
GROUP BY u.first_visit_date, e.variation
ORDER BY u.first_visit_date, e.variation;
            """
            try:
                with engine.connect() as conn:
                    conn.execute(text(insert_query))
                print(f"✅ 分批 {i+1}/{batch_count} 数据已成功写入 {table_name} 中！")
            except SQLAlchemyError as e:
                print(f"🚨 分批 {i+1}/{batch_count} 数据插入失败: {e}")

        # 所有批次数据插入完毕后，进行数据聚合
        merge_query = f"""
        SELECT
            dt,
            variation,
            SUM(new_users) AS new_users,
            SUM(d1) AS d1,
            SUM(d3) AS d3,
            SUM(d7) AS d7,
            SUM(d15) AS d15,
            MAX(total_assigned) AS total_assigned
        FROM {table_name}
        GROUP BY dt, variation;
        """
        aggregated_data = []
        try:
            with engine.connect() as conn:
                result = conn.execute(text(merge_query))
                # 使用 .mappings() 获取字典格式结果（需 SQLAlchemy 1.4+）
                aggregated_data = result.mappings().all()
            print("✅ 数据聚合成功！")
        except SQLAlchemyError as e:
            print(f"🚨 数据聚合失败: {e}")

        # 清空原表中的分批数据（覆盖）
        try:
            with engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE {table_name};"))
            print(f"✅ 表 {table_name} 已成功清空，准备写入聚合后的数据！")
        except SQLAlchemyError as e:
            print(f"🚨 清空数据失败: {e}")

        # 将聚合后的数据重新插入原表中
        for row in aggregated_data:
            insert_row_query = f"""
            INSERT INTO {table_name} (dt, variation, new_users, d1, d3, d7, d15, total_assigned)
            VALUES (:dt, :variation, :new_users, :d1, :d3, :d7, :d15, :total_assigned);
            """
            try:
                with engine.connect() as conn:
                    conn.execute(text(insert_row_query), {
                        'dt': row['dt'],
                        'variation': row['variation'],
                        'new_users': row['new_users'],
                        'd1': row['d1'],
                        'd3': row['d3'],
                        'd7': row['d7'],
                        'd15': row['d15'],
                        'total_assigned': row['total_assigned']
                    })
                print(f"✅ 聚合数据插入 {row['dt']} - {row['variation']} 成功！")
            except SQLAlchemyError as e:
                print(f"🚨 聚合数据插入失败: {e}")

    except Exception as e:
        print(f"🚨 执行失败: {e}")

if __name__ == "__main__":
    tag = "chat_0519"
    insert_experiment_data_to_wide_table(tag)