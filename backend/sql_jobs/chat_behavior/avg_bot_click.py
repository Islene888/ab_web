from sqlalchemy import text
from datetime import datetime, timedelta

def fetch_group_avg_bot_click_samples(experiment_name, start_date, end_date, engine):
    all_results = []
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    delta = timedelta(days=1)
    current_dt = start_dt

    while current_dt <= end_dt:
        current_date_str = current_dt.strftime("%Y-%m-%d")
        query = f"""

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
            user_bot_cnt AS (
                SELECT
                    v.event_date,
                    a.variation_id,
                    v.user_id,
                    COUNT(DISTINCT v.bot_id) AS bot_cnt,
                    CASE WHEN n.user_id IS NOT NULL THEN 1 ELSE 0 END AS is_new_user
                FROM flow_event_info.tbl_app_event_bot_view v
                JOIN dedup_assignment a ON v.user_id = a.user_id AND v.event_date = a.event_date
                LEFT JOIN flow_wide_info.tbl_wide_user_first_visit_app_info n
                    ON v.user_id = n.user_id AND DATE(n.first_visit_date) = v.event_date
                WHERE v.event_date = '{current_date_str}'
                GROUP BY v.event_date, a.variation_id, v.user_id, is_new_user
            )
            SELECT
                '{current_date_str}' AS event_date,
                variation_id,
                SUM(bot_cnt) as total_click,
                COUNT(DISTINCT user_id) AS total_user,
                ROUND(SUM(bot_cnt)*1.0/COUNT(DISTINCT user_id), 4) AS avg_bot_clicked,
                SUM(CASE WHEN is_new_user=1 THEN bot_cnt ELSE 0 END) AS new_user_total_click,
                COUNT(DISTINCT CASE WHEN is_new_user=1 THEN user_id ELSE NULL END) AS new_user_total_user,
                ROUND(
                  SUM(CASE WHEN is_new_user=1 THEN bot_cnt ELSE 0 END)*1.0 /
                  NULLIF(COUNT(DISTINCT CASE WHEN is_new_user=1 THEN user_id ELSE NULL END),0), 4
                ) AS new_user_avg_bot_clicked
            FROM user_bot_cnt
            GROUP BY variation_id;
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

