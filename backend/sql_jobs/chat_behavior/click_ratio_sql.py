from sqlalchemy import text

def fetch_group_click_ratio_samples(experiment_name, start_date, end_date, engine):
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
            WHERE experiment_id = :experiment_name
        ) t
        WHERE rn = 1
    ),
    base_show AS (
        SELECT user_id, event_date, COUNT(distinct event_id) AS show_times
        FROM flow_event_info.tbl_app_event_show_prompt_card
        WHERE event_date BETWEEN :start_date AND :end_date
        GROUP BY user_id, event_date
    ),
    base_view AS (
        SELECT user_id, event_date, COUNT(distinct event_id) AS click_times
        FROM flow_event_info.tbl_app_event_bot_view
        WHERE event_date BETWEEN :start_date AND :end_date
        GROUP BY user_id, event_date
    ),
    new_users AS (
        SELECT user_id
        FROM flow_wide_info.tbl_wide_user_first_visit_app_info
        WHERE DATE(first_visit_date) BETWEEN :start_date AND :end_date
    ),
    joined_data AS (
        SELECT
            a.variation_id,
            s.user_id AS show_user_id,
            s.event_date,
            s.show_times,
            COALESCE(v.click_times, 0) AS click_times,
            CASE WHEN n.user_id IS NOT NULL THEN 1 ELSE 0 END AS is_new_user
        FROM dedup_assignment a
        LEFT JOIN base_show s
            ON a.user_id = s.user_id AND a.event_date = s.event_date
        LEFT JOIN base_view v
            ON s.user_id = v.user_id AND s.event_date = v.event_date
        LEFT JOIN new_users n
            ON s.user_id = n.user_id
    )
    SELECT
        s.event_date,
        a.variation_id,
        SUM(s.show_times) AS showed_events,
        SUM(COALESCE(v.click_times, 0)) AS clicked_events,
        ROUND(SUM(COALESCE(v.click_times, 0)) * 1.0 / NULLIF(SUM(s.show_times), 0), 4) AS click_ratio,
        SUM(CASE WHEN n.user_id IS NOT NULL THEN s.show_times ELSE 0 END) AS new_showed_events,
        SUM(CASE WHEN n.user_id IS NOT NULL THEN COALESCE(v.click_times, 0) ELSE 0 END) AS new_clicked_events,
        ROUND(
            SUM(CASE WHEN n.user_id IS NOT NULL THEN COALESCE(v.click_times, 0) ELSE 0 END) * 1.0 /
            NULLIF(SUM(CASE WHEN n.user_id IS NOT NULL THEN s.show_times ELSE 0 END), 0)
        , 4) AS new_user_click_ratio,
        :experiment_name AS experiment_name
    FROM dedup_assignment a
    LEFT JOIN base_show s ON a.user_id = s.user_id AND a.event_date = s.event_date
    LEFT JOIN base_view v ON s.user_id = v.user_id AND s.event_date = v.event_date
    LEFT JOIN new_users n ON s.user_id = n.user_id
    WHERE s.event_date BETWEEN :start_date AND :end_date
    GROUP BY a.variation_id, s.event_date
    ORDER BY s.event_date, a.variation_id;
    '''
    with engine.connect() as conn:
        df = conn.execute(
            text(query),
            {"experiment_name": experiment_name, "start_date": start_date, "end_date": end_date}
        ).fetchall()
    print(f"CLICK: 实验 {experiment_name} 查询到 {len(df)} 条记录")
    return df