from sqlalchemy import text
from datetime import datetime, timedelta

def fetch_group_time_spend_samples(experiment_name, start_date, end_date, engine):
    all_results = []
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    delta = timedelta(days=1)
    current_dt = start_dt

    while current_dt <= end_dt:
        current_date_str = current_dt.strftime("%Y-%m-%d")
        query = f"""
  WITH session_agg AS (
            SELECT
                DATE(event_date) AS event_date,                                 
                user_id,                                      
                ROUND(SUM(duration) / 1000 / 60, 2) AS total_time_minutes  
            FROM flow_event_info.tbl_app_session_info
            WHERE DATE(event_date) = '{current_date_str}'
            GROUP BY DATE(event_date), user_id
        ),
        experiment_var AS (
            SELECT user_id, variation_id
            FROM (
                SELECT
                    user_id,
                    variation_id,
                    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_date) AS rn
                FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                WHERE experiment_id = '{experiment_name}'
                  AND event_date = '{current_date_str}'
            ) t
            WHERE rn = 1
        ),
        new_users AS (
            SELECT user_id
            FROM flow_wide_info.tbl_wide_user_first_visit_app_info
            WHERE DATE(first_visit_date) = '{current_date_str}'
        )
        SELECT
            sa.event_date,
            ev.variation_id AS variation,
            SUM(sa.total_time_minutes) AS total_time_minutes,
            COUNT(DISTINCT sa.user_id) AS unique_users,
            ROUND(SUM(sa.total_time_minutes) / NULLIF(COUNT(DISTINCT sa.user_id), 0), 2) AS avg_time_spent_minutes,
            SUM(CASE WHEN nu.user_id IS NOT NULL THEN sa.total_time_minutes ELSE 0 END) AS new_user_total_time_minutes,
            COUNT(DISTINCT CASE WHEN nu.user_id IS NOT NULL THEN sa.user_id END) AS new_user_count,
            ROUND(
                SUM(CASE WHEN nu.user_id IS NOT NULL THEN sa.total_time_minutes ELSE 0 END) 
                / NULLIF(COUNT(DISTINCT CASE WHEN nu.user_id IS NOT NULL THEN sa.user_id END), 0), 2
            ) AS new_user_avg_time_spent_minutes,
            '{experiment_name}' AS experiment_name
        FROM session_agg sa
        JOIN experiment_var ev ON sa.user_id = ev.user_id
        LEFT JOIN new_users nu ON sa.user_id = nu.user_id
        GROUP BY sa.event_date, ev.variation_id
        ORDER BY sa.event_date, ev.variation_id;


    """
        with engine.connect() as conn:
            day_result = conn.execute(text(query)).fetchall()
        for row in day_result:
            row_dict = dict(row) if not hasattr(row, '_asdict') else row._asdict()
            row_dict['event_date'] = current_date_str
            all_results.append(row_dict)
        current_dt += delta
    print(f"CLICK: 实验 {experiment_name} 多天合并查询到 {len(all_results)} 条记录")
    return all_results

