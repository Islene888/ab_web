from sqlalchemy import text
from datetime import datetime, timedelta

def fetch_group_cumulative_lt_daily(experiment_name, start_date, end_date, engine):
    """
    查询 [start_date, end_date] 区间内，每一天、每组（variation）的累计 LT（日累计人均时长，单位分钟）。
    返回 list[dict]，每条是 {'event_date', 'variation_id', 'cumulative_time_minutes', 'cumulative_active_users', 'cumulative_lt'}
    """
    all_results = []
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    delta = timedelta(days=1)
    current_dt = start_dt

    while current_dt <= end_dt:
        current_date_str = current_dt.strftime("%Y-%m-%d")

        query = f"""
        WITH
            exp AS (
                SELECT user_id, variation_id, event_date
                FROM (
                    SELECT
                        user_id,
                        variation_id,
                        event_date,
                        ROW_NUMBER() OVER (PARTITION BY user_id, event_date ORDER BY event_date DESC) AS rn
                    FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                    WHERE experiment_id = '{experiment_name}'
                        AND event_date <= '{current_date_str}'
                ) t
                WHERE rn = 1
            ),
            session AS (
                SELECT user_id, event_date, SUM(duration) / 1000 / 60 AS time_minutes
                FROM flow_event_info.tbl_app_session_info
                WHERE event_date <= '{current_date_str}'
                GROUP BY user_id, event_date
            ),
            user_daily AS (
                SELECT
                    e.user_id,
                    e.variation_id,
                    e.event_date,
                    COALESCE(s.time_minutes, 0) AS time_minutes
                FROM exp e
                LEFT JOIN session s ON e.user_id = s.user_id AND e.event_date = s.event_date
            )
        SELECT
            '{current_date_str}' AS event_date,
            variation_id,
            SUM(time_minutes) AS cumulative_time_minutes,
            COUNT(DISTINCT user_id) AS cumulative_users,
            ROUND(SUM(time_minutes) / NULLIF(COUNT(DISTINCT user_id),0), 2) AS cumulative_lt
        FROM user_daily
        GROUP BY variation_id;
        """

        with engine.connect() as conn:
            day_result = conn.execute(text(query)).fetchall()
        for row in day_result:
            if hasattr(row, '_asdict'):
                row_dict = row._asdict()
            else:
                row_dict = dict(row)
            all_results.append(row_dict)
        current_dt += delta

    print(f"[Cohort Cumulative LT] 实验 {experiment_name}: {len(all_results)} 条记录")
    return all_results
