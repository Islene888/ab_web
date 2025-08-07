
from sqlalchemy import text
from datetime import datetime, timedelta
def fetch_group_cumulative_lt_daily(experiment_name, start_date, end_date, engine):
    """
    查询 [start_date, end_date] 区间内，每一天、每组（variation）的累计 LT（日累计人均时长，单位分钟）。
    字段：event_date, variation_id, cumulative_time_minutes, cumulative_active_users, cumulative_lt
    """
    query = f"""
      WITH user_variation_map AS (
        SELECT user_id, variation_id FROM (
          SELECT user_id, variation_id,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_date ASC) AS rn
          FROM flow_wide_info.tbl_wide_experiment_assignment_hi
          WHERE experiment_id = '{experiment_name}'
            AND event_date BETWEEN '{start_date}' AND '{end_date}'
        ) t WHERE rn = 1
      ),
      session_base AS (
        SELECT
          DATE(s.event_date) AS event_date,
          s.user_id,
          uvm.variation_id,
          SUM(s.duration) / 1000 / 60 AS time_minutes
        FROM flow_event_info.tbl_app_session_info s
        JOIN user_variation_map uvm ON s.user_id = uvm.user_id
        WHERE s.event_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY DATE(s.event_date), s.user_id, uvm.variation_id
      ),
      user_first_active AS (
        SELECT user_id, variation_id, MIN(event_date) AS first_active_date
        FROM session_base GROUP BY user_id, variation_id
      ),
      date_variation AS (
        SELECT DISTINCT event_date, variation_id FROM session_base
      ),
      user_cumulative_by_day AS (
        SELECT dv.event_date, dv.variation_id,
          COUNT(*) AS cumulative_active_users
        FROM date_variation dv
        JOIN user_first_active ufa
          ON ufa.variation_id = dv.variation_id
         AND ufa.first_active_date <= dv.event_date
        GROUP BY dv.event_date, dv.variation_id
      ),
      user_time_per_day AS (
        SELECT event_date, variation_id, SUM(time_minutes) AS time_minutes
        FROM session_base GROUP BY event_date, variation_id
      ),
      time_cumulative_by_day AS (
        SELECT event_date, variation_id,
          SUM(time_minutes) OVER (PARTITION BY variation_id ORDER BY event_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_time_minutes
        FROM user_time_per_day
      )
      SELECT
        t.event_date,
        t.variation_id,
        t.cumulative_time_minutes,
        u.cumulative_active_users as cumulative_users,
        ROUND(t.cumulative_time_minutes / NULLIF(u.cumulative_active_users,0), 2) AS cumulative_lt
      FROM time_cumulative_by_day t
      JOIN user_cumulative_by_day u
        ON t.event_date = u.event_date AND t.variation_id = u.variation_id
      ORDER BY t.event_date, t.variation_id
    """

    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchall()
    all_results = [dict(row._mapping) for row in result]  # 就改这一行！
    print(f"[Cohort Cumulative LT] 实验 {experiment_name}: {len(all_results)} 条记录")
    return all_results