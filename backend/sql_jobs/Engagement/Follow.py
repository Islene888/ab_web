from sqlalchemy import text
from datetime import datetime, timedelta

def fetch_group_follow_samples(experiment_name, start_date, end_date, engine):
    all_results = []
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    delta = timedelta(days=1)
    current_dt = start_dt

    while current_dt <= end_dt:
        current_date_str = current_dt.strftime("%Y-%m-%d")
        query = f'''
            WITH dedup_assign AS (
                SELECT user_id, variation_id
                FROM (
                    SELECT
                        user_id,
                        variation_id,
                        event_date,
                        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_date DESC) AS rn
                    FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                    WHERE experiment_id = '{experiment_name}'
                ) t
                WHERE rn = 1
            )
            SELECT
                f.event_date,
                a.variation_id AS variation,
                COUNT(DISTINCT f.event_id) AS total_follow,
                COUNT(DISTINCT f.user_id) AS unique_follow_users,
                CASE
                    WHEN COUNT(DISTINCT f.user_id) = 0 THEN 0
                    ELSE ROUND(COUNT(DISTINCT f.event_id) * 1.0 / COUNT(DISTINCT f.user_id), 4)
                END AS follow_ratio
            FROM flow_event_info.tbl_app_event_bot_follow f
            JOIN dedup_assign a ON f.user_id = a.user_id
            WHERE f.event_date = '{current_date_str}'
            GROUP BY f.event_date, a.variation_id
        '''
        with engine.connect() as conn:
            day_result = conn.execute(text(query)).fetchall()
        for row in day_result:
            row_dict = dict(row) if not hasattr(row, '_asdict') else row._asdict()
            row_dict['event_date'] = current_date_str
            all_results.append(row_dict)
        current_dt += delta
    print(f"CLICK: 实验 {experiment_name} 多天合并查询到 {len(all_results)} 条记录")
    return all_results
