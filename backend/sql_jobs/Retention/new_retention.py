from sqlalchemy import text
from datetime import datetime, timedelta

def fetch_new_user_retention(engine, experiment_name, start_date, end_date):
    all_results = []
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    delta = timedelta(days=1)
    current_dt = start_dt

    while current_dt <= end_dt:
        current_date_str = current_dt.strftime("%Y-%m-%d")
        query = f'''
            /*+ SET_VAR (query_timeout = 30000) */ 
            SELECT
                u.first_visit_date AS dt, 
                e.variation, 
                COUNT(DISTINCT u.user_id) AS new_users,
                COUNT(DISTINCT CASE WHEN DATEDIFF(a.active_date, u.first_visit_date) = 1 THEN a.user_id END) AS d1,
                COUNT(DISTINCT CASE WHEN DATEDIFF(a.active_date, u.first_visit_date) = 3 THEN a.user_id END) AS d3,
                COUNT(DISTINCT CASE WHEN DATEDIFF(a.active_date, u.first_visit_date) = 7 THEN a.user_id END) AS d7,
                COUNT(DISTINCT CASE WHEN DATEDIFF(a.active_date, u.first_visit_date) = 15 THEN a.user_id END) AS d15,
                MAX(COALESCE(ta.total_assigned, 0)) AS total_assigned
            FROM (
                SELECT 
                    user_id,
                    DATE(first_visit_date) AS first_visit_date
                FROM flow_wide_info.tbl_wide_user_first_visit_app_info
                WHERE first_visit_date = '{current_date_str}'
            ) u
            LEFT JOIN (
                SELECT
                    d.user_id,
                    d.active_date
                FROM flow_wide_info.tbl_wide_active_user_app_info d
                WHERE
                    d.active_date BETWEEN '{current_date_str}' AND DATE_ADD('{current_date_str}', INTERVAL 15 DAY)
                    AND d.keep_alive_flag = 1
                    AND d.user_id IS NOT NULL
                    AND d.user_id != ''
                GROUP BY d.active_date, d.user_id
            ) a ON u.user_id = a.user_id
            LEFT JOIN (
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
                ) t
                WHERE rn = 1
            ) e ON u.user_id = e.user_id
            LEFT JOIN (
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
            WHERE e.variation IS NOT NULL
            GROUP BY u.first_visit_date, e.variation
            ORDER BY u.first_visit_date, e.variation;
        '''
        with engine.connect() as conn:
            day_result = conn.execute(text(query)).fetchall()
        for row in day_result:
            row_dict = dict(row) if not hasattr(row, '_asdict') else row._asdict()
            row_dict['first_visit_date'] = current_date_str  # 明确标记
            all_results.append(row_dict)
        current_dt += delta

    print(f"NEW RETENTION: 实验 {experiment_name} 多天合并查询到 {len(all_results)} 条记录")
    return all_results
