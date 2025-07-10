from sqlalchemy import text
from datetime import datetime, timedelta

def fetch_group_explore_chat_start_rate_samples(experiment_name, start_date, end_date, engine):
    all_results = []
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    delta = timedelta(days=1)
    current_dt = start_dt

    while current_dt <= end_dt:
        current_date_str = current_dt.strftime("%Y-%m-%d")
        query = f'''
            WITH dedup_assignment AS (
                SELECT user_id, event_date, variation_id
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY user_id, event_date, experiment_id ORDER BY variation_id) AS rn
                    FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                    WHERE experiment_id = '{experiment_name}'
                ) t
                WHERE rn = 1
            ),
            base_view AS (
                SELECT DISTINCT user_id, event_date
                FROM flow_event_info.tbl_app_event_bot_view
                WHERE event_date = '{current_date_str}' AND source = 'tag:Explore'
            ),
            base_chat AS (
                SELECT DISTINCT user_id, event_date
                FROM flow_event_info.tbl_app_event_chat_send
                WHERE event_date = '{current_date_str}' AND source = 'tag:Explore'
            ),
            new_users AS (
                SELECT DISTINCT user_id
                FROM flow_wide_info.tbl_wide_user_first_visit_app_info
                WHERE DATE(first_visit_date) = '{current_date_str}'
            ),
            joined AS (
                SELECT
                    a.variation_id,
                    v.user_id,
                    CASE WHEN c.user_id IS NOT NULL THEN 1 ELSE 0 END AS has_chat,
                    CASE WHEN n.user_id IS NOT NULL THEN 1 ELSE 0 END AS is_new_user
                FROM dedup_assignment a
                JOIN base_view v
                    ON a.user_id = v.user_id AND a.event_date = v.event_date
                LEFT JOIN base_chat c
                    ON v.user_id = c.user_id AND v.event_date = c.event_date
                LEFT JOIN new_users n
                    ON v.user_id = n.user_id
            )
            SELECT
                '{current_date_str}' AS event_date,
                variation_id,
                COUNT(DISTINCT user_id) AS clicked_users,
                COUNT(DISTINCT CASE WHEN has_chat = 1 THEN user_id END) AS chat_users,
                CASE WHEN COUNT(DISTINCT user_id) = 0 THEN 0
                    ELSE ROUND(
                        COUNT(DISTINCT CASE WHEN has_chat = 1 THEN user_id END) * 1.0 / 
                        COUNT(DISTINCT user_id), 4)
                END AS chat_start_rate,
                COUNT(DISTINCT CASE WHEN is_new_user = 1 THEN user_id END) AS new_clicked_users,
                COUNT(DISTINCT CASE WHEN is_new_user = 1 AND has_chat = 1 THEN user_id END) AS new_chat_users,
                CASE WHEN COUNT(DISTINCT CASE WHEN is_new_user = 1 THEN user_id END) = 0 THEN 0
                    ELSE ROUND(
                        COUNT(DISTINCT CASE WHEN is_new_user = 1 AND has_chat = 1 THEN user_id END) * 1.0 /
                        COUNT(DISTINCT CASE WHEN is_new_user = 1 THEN user_id END), 4)
                END AS new_chat_start_rate,
                '{experiment_name}' AS experiment_name
            FROM joined
            GROUP BY variation_id;
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

