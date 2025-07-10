from sqlalchemy import text

def fetch_active_user_retention(engine, experiment_name, start_time, end_time):
    query = f'''
        SELECT
            base.active_date AS 活跃用户日期,
            e.variation AS 实验组,
            COUNT(DISTINCT base.user_id) AS 活跃用户数,
            COUNT(DISTINCT d1.user_id) AS d1留存活跃用户数,
            COUNT(DISTINCT d3.user_id) AS d3留存活跃用户数,
            COUNT(DISTINCT d7.user_id) AS d7留存活跃用户数,
            COUNT(DISTINCT d15.user_id) AS d15留存活跃用户数,
            ROUND(COUNT(DISTINCT d1.user_id) / NULLIF(COUNT(DISTINCT base.user_id), 0), 4) AS d1留存率,
            ROUND(COUNT(DISTINCT d3.user_id) / NULLIF(COUNT(DISTINCT base.user_id), 0), 4) AS d3留存率,
            ROUND(COUNT(DISTINCT d7.user_id) / NULLIF(COUNT(DISTINCT base.user_id), 0), 4) AS d7留存率,
            ROUND(COUNT(DISTINCT d15.user_id) / NULLIF(COUNT(DISTINCT base.user_id), 0), 4) AS d15留存率,
            MAX(COALESCE(ta.total_assigned, 0)) AS 当日分配实验用户数
        FROM (
            SELECT user_id, active_date
            FROM flow_wide_info.tbl_wide_active_user_app_info
            WHERE active_date BETWEEN '{start_time}' AND '{end_time}'
              AND keep_alive_flag = 1
              AND user_id IS NOT NULL AND user_id != ''
        ) base
        LEFT JOIN (
            SELECT user_id, CAST(variation_id AS CHAR) AS variation
            FROM (
                SELECT user_id, variation_id,
                    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp_assigned ASC) AS rn
                FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                WHERE experiment_id = '{experiment_name}'
                  AND timestamp_assigned BETWEEN '{start_time}' AND '{end_time}'
            ) t
            WHERE rn = 1
        ) e ON base.user_id = e.user_id
        LEFT JOIN (
            SELECT user_id, active_date
            FROM flow_wide_info.tbl_wide_active_user_app_info
            WHERE active_date BETWEEN DATE_ADD('{start_time}', INTERVAL 1 DAY) AND DATE_ADD('{end_time}', INTERVAL 15 DAY)
              AND keep_alive_flag = 1
        ) d1 ON base.user_id = d1.user_id AND DATEDIFF(d1.active_date, base.active_date) = 1
        LEFT JOIN (
            SELECT user_id, active_date
            FROM flow_wide_info.tbl_wide_active_user_app_info
            WHERE active_date BETWEEN DATE_ADD('{start_time}', INTERVAL 3 DAY) AND DATE_ADD('{end_time}', INTERVAL 15 DAY)
              AND keep_alive_flag = 1
        ) d3 ON base.user_id = d3.user_id AND DATEDIFF(d3.active_date, base.active_date) = 3
        LEFT JOIN (
            SELECT user_id, active_date
            FROM flow_wide_info.tbl_wide_active_user_app_info
            WHERE active_date BETWEEN DATE_ADD('{start_time}', INTERVAL 7 DAY) AND DATE_ADD('{end_time}', INTERVAL 15 DAY)
              AND keep_alive_flag = 1
        ) d7 ON base.user_id = d7.user_id AND DATEDIFF(d7.active_date, base.active_date) = 7
        LEFT JOIN (
            SELECT user_id, active_date
            FROM flow_wide_info.tbl_wide_active_user_app_info
            WHERE active_date BETWEEN DATE_ADD('{start_time}', INTERVAL 15 DAY) AND DATE_ADD('{end_time}', INTERVAL 15 DAY)
              AND keep_alive_flag = 1
        ) d15 ON base.user_id = d15.user_id AND DATEDIFF(d15.active_date, base.active_date) = 15
        LEFT JOIN (
            SELECT assign_date, variation, COUNT(DISTINCT user_id) AS total_assigned
            FROM (
                SELECT user_id, DATE(timestamp_assigned) AS assign_date,
                    CAST(variation_id AS CHAR) AS variation,
                    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp_assigned ASC) AS rn
                FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                WHERE experiment_id = '{experiment_name}'
            ) t
            WHERE rn = 1
            GROUP BY assign_date, variation
        ) ta ON ta.assign_date = base.active_date AND ta.variation = e.variation
        WHERE e.variation IS NOT NULL
        GROUP BY base.active_date, e.variation
        ORDER BY base.active_date, e.variation;
    '''
    with engine.connect() as conn:
        df = conn.execute(text(query)).fetchall()

    return df 