from sqlalchemy import text
from datetime import datetime, timedelta

def fetch_group_explore_chat_round_samples(experiment_name, start_date, end_date, engine):
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
                    ROW_NUMBER() OVER (
                        PARTITION BY user_id, event_date, experiment_id
                        ORDER BY variation_id
                    ) AS rn
                FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                WHERE experiment_id = '{experiment_name}'
            ) t
            WHERE rn = 1
        ),
        first_visit_user AS (
            SELECT user_id, DATE(first_visit_date) AS first_visit_date
            FROM flow_wide_info.tbl_wide_user_first_visit_app_info
        ),
        chat_data AS (
            SELECT
                cs.*,
                a.variation_id,
                u.user_id AS new_user_flag,
                CASE WHEN u.user_id IS NOT NULL AND cs.event_date = u.first_visit_date THEN 1 ELSE 0 END AS is_new_user
            FROM flow_event_info.tbl_app_event_chat_send cs
            JOIN dedup_assignment a
                ON cs.user_id = a.user_id AND cs.event_date = a.event_date
            LEFT JOIN first_visit_user u
                ON cs.user_id = u.user_id
            WHERE cs.event_date = '{current_date_str}'
            AND cs.source = 'tag:Explore'
        )
        SELECT
            '{current_date_str}' AS event_date,
            variation_id AS variation,
            COUNT(event_id) AS total_chat_rounds,
            COUNT(DISTINCT user_id) AS unique_users,
            ROUND(COUNT(distinct event_id) * 1.0 / COUNT(DISTINCT user_id), 2) AS chat_depth_user,
            ROUND(COUNT(DISTINCT CASE WHEN is_new_user = 1 THEN event_id END) * 1.0 / COUNT(DISTINCT CASE WHEN is_new_user = 1 THEN user_id END), 2) AS chat_depth_user_new,
            ROUND(COUNT(DISTINCT CASE WHEN is_new_user = 1 THEN event_id END) * 1.0 / (
                COUNT(DISTINCT prompt_id) * COUNT(DISTINCT CASE WHEN is_new_user = 1 THEN user_id END)
            ), 4) AS chat_depth_per_user_per_bot_new,
            '{experiment_name}' AS experiment_name
        FROM chat_data
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

