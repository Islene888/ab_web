from sqlalchemy import text
from datetime import datetime, timedelta

def fetch_group_new_conversation_samples(experiment_name, start_date, end_date, engine):
    all_results = []
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    delta = timedelta(days=1)
    current_dt = start_dt

    while current_dt <= end_dt:
        current_date_str = current_dt.strftime("%Y-%m-%d")
        query = f'''
           WITH assigned_users AS (
                    SELECT DISTINCT user_id, variation_id
                    FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                    WHERE experiment_id = '{experiment_name}'
                      AND event_date = '{current_date_str}'
                ),
                chat_events AS (
                    SELECT DISTINCT user_id, conversation_id, event_date
                    FROM flow_event_info.tbl_app_event_chat_send
                    WHERE event_date = '{current_date_str}'
                )
                SELECT
                    e.event_date,
                    u.variation_id AS variation,
                    COUNT(DISTINCT e.conversation_id) AS total_new_conversation,
                    COUNT(DISTINCT e.user_id) AS unique_new_conversation_users,
                    CASE
                        WHEN COUNT(DISTINCT e.user_id) = 0 THEN 0
                        ELSE ROUND(COUNT(DISTINCT e.conversation_id) * 1.0 / COUNT(DISTINCT e.user_id), 4)
                    END AS new_conversation_ratio,
                    '{experiment_name}' AS experiment_name
                FROM chat_events e
                JOIN assigned_users u
                  ON e.user_id = u.user_id
                GROUP BY e.event_date, u.variation_id
                ORDER BY e.event_date, u.variation_id;

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

