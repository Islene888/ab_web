from sqlalchemy import text
from datetime import datetime, timedelta

def fetch_group_explore_click_rate_samples(experiment_name, start_date, end_date, engine):
    all_results = []
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    delta = timedelta(days=1)
    current_dt = start_dt

    while current_dt <= end_dt:
        current_date_str = current_dt.strftime("%Y-%m-%d")
        query = f"""
             WITH experiment_assignment_dedup AS (
                SELECT *
                FROM (
                    SELECT
                        user_id,
                        experiment_id,
                        variation_id,
                        event_date,
                        ROW_NUMBER() OVER (PARTITION BY user_id, experiment_id ORDER BY event_date ASC) AS rn
                    FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                    WHERE experiment_id = '{experiment_name}'
                ) t
                WHERE t.rn = 1
            ),
            first_visit_user AS (
                SELECT user_id, DATE(first_visit_date) AS first_visit_date
                FROM flow_wide_info.tbl_wide_user_first_visit_app_info
            ),
            show_info AS (
                SELECT user_id, COUNT(DISTINCT event_id) AS shows
                FROM flow_event_info.tbl_app_event_show_prompt_card
                WHERE event_date = '{current_date_str}'
                  AND current_page = 'home'
                  AND tab_name = 'Explore'
                GROUP BY user_id
            ),
            click_info AS (
                SELECT user_id, COUNT(DISTINCT event_id) AS clicks
                FROM flow_event_info.tbl_app_event_bot_view
                WHERE event_date = '{current_date_str}'
                  AND source = 'tag:Explore'
                GROUP BY user_id
            ),
            raw_data AS (
                SELECT
                    '{current_date_str}' AS event_date,
                    ea.variation_id AS variation,
                    ea.user_id AS user_id,
                    COALESCE(s.shows, 0) AS shows,
                    COALESCE(c.clicks, 0) AS clicks,
                    CASE WHEN u.user_id IS NOT NULL AND u.first_visit_date = '{current_date_str}' THEN 1 ELSE 0 END AS is_new_user
                FROM experiment_assignment_dedup ea
                LEFT JOIN show_info s ON ea.user_id = s.user_id
                LEFT JOIN click_info c ON ea.user_id = c.user_id
                LEFT JOIN first_visit_user u ON ea.user_id = u.user_id
            )
            SELECT
                event_date,
                variation,
                COUNT(DISTINCT user_id) AS show_users,
                ROUND(SUM(shows) * 1.0 / NULLIF(COUNT(DISTINCT user_id), 0), 4) AS avg_shows_per_user,
               
                SUM(shows) AS total_shows,          -- 总展示数
                SUM(clicks) AS total_clicks,        -- 总点击数
                ROUND(SUM(clicks) * 1.0 / NULLIF(SUM(shows), 0), 4) AS click_rate, -- 点击率
            
                COUNT(DISTINCT CASE WHEN is_new_user = 1 THEN user_id END) AS new_user_show_users,
                ROUND(SUM(CASE WHEN is_new_user = 1 THEN shows ELSE 0 END) * 1.0 / NULLIF(COUNT(DISTINCT CASE WHEN is_new_user = 1 THEN user_id END), 0), 4) AS avg_shows_per_new_user,
                ROUND(SUM(CASE WHEN is_new_user = 1 THEN clicks ELSE 0 END) * 1.0 / NULLIF(SUM(CASE WHEN is_new_user = 1 THEN shows ELSE 0 END), 0), 4) AS click_rate_new_user,
            
                '{experiment_name}' AS experiment_name
            FROM raw_data
            GROUP BY event_date, variation;

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

