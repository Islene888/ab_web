from sqlalchemy import text

def fetch_group_explore_chat_start_rate_samples(experiment_name, start_date, end_date, engine):
    query = '''

                WITH dedup_assignment AS (
                SELECT user_id, event_date, variation_id
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY user_id, event_date, experiment_id
                            ORDER BY timestamp_assigned
                        ) AS rn
                    FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                    WHERE experiment_id = '{experiment_name}'
                ) t
                WHERE rn = 1
            ),
            new_user AS (
                SELECT user_id
                FROM flow_wide_info.tbl_wide_user_first_visit_app_info
                WHERE DATE(first_visit_date) = '{current_date}'
            )
            SELECT
                '{current_date}' AS event_date,
                d.variation_id AS variation_id,
                COUNT(DISTINCT CONCAT(c.user_id, '_', c.prompt_id)) AS total_click,
                COUNT(DISTINCT c.user_id) AS total_user,
                ROUND(COUNT(DISTINCT CONCAT(c.user_id, '_', c.prompt_id)) * 1.0 / NULLIF(COUNT(DISTINCT c.user_id), 0), 4) AS avg_bot_clicked,
                COUNT(DISTINCT CASE WHEN n.user_id IS NOT NULL THEN CONCAT(c.user_id, '_', c.prompt_id) END) AS new_user_total_click,
                COUNT(DISTINCT CASE WHEN n.user_id IS NOT NULL THEN c.user_id END) AS new_user_total_user,
                ROUND(
                    COUNT(DISTINCT CASE WHEN n.user_id IS NOT NULL THEN CONCAT(c.user_id, '_', c.prompt_id) END) * 1.0 /
                    NULLIF(COUNT(DISTINCT CASE WHEN n.user_id IS NOT NULL THEN c.user_id END), 0)
                , 4) AS new_user_avg_bot_clicked
            FROM flow_event_info.tbl_app_event_chat_send c
            JOIN dedup_assignment d
              ON c.user_id = d.user_id AND c.event_date = d.event_date
            LEFT JOIN new_user n
              ON c.user_id = n.user_id
            WHERE c.event_date = '{current_date}'
            GROUP BY d.variation_id
    '''
    with engine.connect() as conn:
        df = conn.execute(
            text(query),
            {"experiment_name": experiment_name, "start_date": start_date, "end_date": end_date}
        ).fetchall()
    print(f"CLICK: 实验 {experiment_name} 查询到 {len(df)} 条记录")
    return df

