from sqlalchemy import text

def fetch_cohort_time_spent_heatmap(experiment_name, start_date, end_date, engine, max_days=30):
    """
    Cohort 新用户注册留存日均时长热力图
    返回结构: [
      {
        "variation_id": ...,
        "register_date": ...,
        "cohort_day": ...,
        "avg_time_spent_minutes": ...,
        "total_time_spent": ...,
        "active_users": ...
      }, ...
    ]
    """
    query = f"""
    WITH
      -- 找到cohort用户的注册日
      first_active AS (
        SELECT
          e.user_id,
          e.variation_id,
          MIN(a.active_date) AS register_date
        FROM flow_wide_info.tbl_wide_active_user_app_info a
        JOIN (
          SELECT user_id, variation_id
          FROM (
            SELECT user_id, variation_id,
                   ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp_assigned ASC) AS rn
            FROM flow_wide_info.tbl_wide_experiment_assignment_hi
            WHERE experiment_id = '{experiment_name}'
          ) t
          WHERE rn = 1
        ) e ON a.user_id = e.user_id
        WHERE a.active_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY e.user_id, e.variation_id
      ),
      -- 每个cohort注册日后，统计每天的活跃及时长
      cohort_session AS (
        SELECT
          fa.variation_id,
          fa.register_date,
          s.event_date,
          DATEDIFF(s.event_date, fa.register_date) AS cohort_day,
          s.user_id,
          SUM(s.duration) / 1000 / 60 AS time_minutes
        FROM first_active fa
        JOIN flow_event_info.tbl_app_session_info s
          ON fa.user_id = s.user_id
         AND s.event_date BETWEEN fa.register_date AND DATE_ADD(fa.register_date, INTERVAL {max_days} DAY)
         AND DATEDIFF(s.event_date, fa.register_date) BETWEEN 0 AND {max_days}
        GROUP BY fa.variation_id, fa.register_date, s.event_date, s.user_id
      )
    SELECT
      cs.variation_id,
      cs.register_date,
      cs.cohort_day,
      SUM(cs.time_minutes) AS total_time_spent,
      COUNT(DISTINCT cs.user_id) AS active_users,
      ROUND(SUM(cs.time_minutes) / NULLIF(COUNT(DISTINCT cs.user_id), 0), 2) AS avg_time_spent_minutes
    FROM cohort_session cs
    GROUP BY cs.variation_id, cs.register_date, cs.cohort_day
    ORDER BY cs.variation_id, cs.register_date, cs.cohort_day
    ;
    """
    with engine.connect() as conn:
        df = conn.execute(text(query)).fetchall()
    print(f"[COHORT-TIME-SPENT-HEATMAP] 实验 {experiment_name} 查询到 {len(df)} 条记录")
    return df
