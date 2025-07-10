from sqlalchemy import text
from datetime import datetime, timedelta

def fetch_group_continue_samples(experiment_name, start_date, end_date, engine):
    all_results = []
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    delta = timedelta(days=1)
    current_dt = start_dt

    while current_dt <= end_dt:
        current_date_str = current_dt.strftime("%Y-%m-%d")
        query = f'''
                 WITH dedup_assign AS (
                SELECT user_id, variation_id, event_date
                FROM (
                    SELECT
                        user_id,
                        variation_id,
                        event_date,
                        ROW_NUMBER() OVER (PARTITION BY user_id, event_date ORDER BY timestamp_assigned ASC) AS rn
                    FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                    WHERE experiment_id = '{experiment_name}'
                ) t
                WHERE rn = 1 AND event_date = '{current_date_str}'
            )
            SELECT
                a.event_date,
                b.variation_id AS variation,
                COUNT(DISTINCT a.event_id) AS total_continue,
                COUNT(DISTINCT a.user_id) AS unique_continue_users,
                CASE
                    WHEN COUNT(DISTINCT a.user_id) = 0 THEN 0
                    ELSE ROUND(COUNT(DISTINCT a.event_id) * 1.0 / COUNT(DISTINCT a.user_id), 4)
                END AS continue_ratio,
                '{experiment_name}' AS experiment_name
            FROM flow_event_info.tbl_app_event_chat_send a
            JOIN dedup_assign b
              ON a.user_id = b.user_id
             AND a.event_date = b.event_date
            WHERE a.event_date = '{current_date_str}'
              AND a.Method = 'continue'
            GROUP BY a.event_date, b.variation_id
            ORDER BY a.event_date, b.variation_id;

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

