from sqlalchemy import text
from datetime import datetime

def fetch_group_cumulative_retained_users_daily(experiment_name, start_date, end_date, engine):
    """
    【逻辑修正版】
    一次性查询出每天的累计留存用户数、累计注册用户数和累计留存率，分 variation 输出。
    修复了导致累计留存率下降的逻辑问题。
    """
    query = """
    WITH user_first_active AS (
        -- 步骤1: 先计算出每个用户的首次活跃日
        SELECT
            e.user_id,
            e.variation_id,
            MIN(a.active_date) AS first_active_date
        FROM flow_wide_info.tbl_wide_active_user_app_info a
        JOIN (
            SELECT user_id, variation_id FROM (
                SELECT user_id, variation_id, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp_assigned ASC) AS rn
                FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                WHERE experiment_id = :experiment_name
            ) t WHERE rn = 1
        ) e ON a.user_id = e.user_id
        GROUP BY e.user_id, e.variation_id
    ),
    user_activity_summary AS (
        -- 步骤2: 基于首次活跃日，计算用户的首次“留存日”
        SELECT
            ufa.user_id,
            ufa.variation_id,
            ufa.first_active_date,
            MIN(a.active_date) as retained_date -- 找到在首次活跃日之后，最早的那次活跃日期
        FROM user_first_active ufa
        LEFT JOIN flow_wide_info.tbl_wide_active_user_app_info a
            ON ufa.user_id = a.user_id AND a.active_date > ufa.first_active_date
        GROUP BY ufa.user_id, ufa.variation_id, ufa.first_active_date
    ),
    date_series AS (
        -- (生成日期序列，逻辑不变)
        SELECT DATE_ADD(:start_date, INTERVAL seq.seq DAY) AS event_date FROM (
            SELECT (HUNDREDS.digit * 100 + TENS.digit * 10 + ONES.digit) AS seq FROM
            (SELECT 0 AS digit UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) ONES
            CROSS JOIN (SELECT 0 AS digit UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) TENS
            CROSS JOIN (SELECT 0 AS digit UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) HUNDREDS
        ) seq WHERE DATE_ADD(:start_date, INTERVAL seq.seq DAY) <= :end_date
    ),
    daily_cumulative_metrics AS (
        -- 步骤3: 聚合计算每日的累计指标
        SELECT
            d.event_date,
            uas.variation_id,
            COUNT(DISTINCT CASE WHEN uas.first_active_date <= d.event_date THEN uas.user_id END) AS cumulative_registered_users,
            COUNT(DISTINCT CASE WHEN uas.retained_date IS NOT NULL AND uas.retained_date <= d.event_date THEN uas.user_id END) AS cumulative_retained_users
        FROM date_series d
        CROSS JOIN (SELECT DISTINCT variation_id FROM user_activity_summary) vars
        LEFT JOIN user_activity_summary uas ON vars.variation_id = uas.variation_id
        GROUP BY d.event_date, uas.variation_id
    )
    -- 最终查询
    SELECT
        m.event_date,
        m.variation_id,
        m.cumulative_retained_users,
        m.cumulative_registered_users,
        ROUND(m.cumulative_retained_users * 1.0 / NULLIF(m.cumulative_registered_users, 0), 4) AS cumulative_retention_rate
    FROM daily_cumulative_metrics m
    WHERE m.variation_id IS NOT NULL
    ORDER BY m.variation_id, m.event_date;
    """
    params = {"experiment_name": experiment_name, "start_date": start_date, "end_date": end_date}
    with engine.connect() as conn:
        result_proxy = conn.execute(text(query), params)
        all_results = [dict(row._mapping) for row in result_proxy]
    print(f"Cumulative Retained Users (每日): 实验 {experiment_name} 单次高效查询到 {len(all_results)} 条记录")
    return all_results