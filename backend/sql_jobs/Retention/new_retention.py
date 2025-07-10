from sqlalchemy import text

def fetch_new_user_retention(engine, experiment_name, start_time, end_time):
    query = f'''
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
            WHERE first_visit_date BETWEEN '{start_time}' AND '{end_time}'
        ) u
        LEFT JOIN (
            -- 活跃用户行为表，区间扩大到end_time+15天以覆盖所有留存天数
            SELECT
                d.user_id,
                d.active_date
            FROM flow_wide_info.tbl_wide_active_user_app_info d
            WHERE
                d.active_date BETWEEN '{start_time}' AND DATE_ADD('{end_time}', INTERVAL 15 DAY)
                AND d.keep_alive_flag = 1
                AND d.user_id IS NOT NULL
                AND d.user_id != ''
            GROUP BY d.active_date, d.user_id
        ) a ON u.user_id = a.user_id
        LEFT JOIN (
            -- 实验分组：每个user_id只保留最早的分配记录
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
            -- 分组总分配人数（去重后）
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
        -- 排除未分组用户
        WHERE e.variation IS NOT NULL
        GROUP BY u.first_visit_date, e.variation
        ORDER BY u.first_visit_date, e.variation;
    '''
    with engine.connect() as conn:
        df = conn.execute(text(query)).fetchall()
    print('d1 rows:', df)
    print('group_bayes value_list:', [row[2] for row in df])
    print('trend d1:', [row[3] for row in df])
    return df 