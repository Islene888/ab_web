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
    query = """
           WITH
          -- 步骤1: 定义我们的“定点群组”。
          -- 即，在指定的 start_date 当天，所有活跃过的实验用户。
          golden_cohort_users AS (
            SELECT DISTINCT
              e.user_id,
              e.variation_id
            FROM flow_wide_info.tbl_wide_active_user_app_info a
            JOIN (
              SELECT user_id, variation_id
              FROM (
                SELECT user_id, variation_id,
                       ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp_assigned ASC) AS rn
                FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                WHERE experiment_id = :experiment_name
              ) t
              WHERE rn = 1
            ) e ON a.user_id = e.user_id
            -- 关键筛选：只选择在 start_date 当天活跃的用户
            WHERE a.active_date = :start_date
          ),

          -- 步骤2: 获取这个“定点群组”在整个分析周期内的所有会话记录
          daily_sessions AS (
            SELECT
              gcu.variation_id,
              s.user_id,
              s.event_date,
              s.duration
            FROM golden_cohort_users gcu
            JOIN flow_event_info.tbl_app_session_info s ON gcu.user_id = s.user_id
            -- 筛选出会话日期在指定分析范围内的记录
            WHERE s.event_date BETWEEN :start_date AND :end_date
          )

        -- 步骤3: 按天聚合，计算每日的人均使用时长
        SELECT
          ds.variation_id,
          -- “注册日期”在这里作为一个标签，代表我们定点群组的基准日
          :start_date AS register_date,
          -- 计算今天是追踪周期的第几天 (Day 0, Day 1, ...)
          DATEDIFF(ds.event_date, :start_date) AS cohort_day,
          -- 当天的总使用时长（分钟）
          SUM(ds.duration) / 1000.0 / 60.0 AS total_time_spent,
          -- 当天回访的活跃用户数 (来自我们的定点群组)
          COUNT(DISTINCT ds.user_id) AS active_users,
          -- 计算“人均日活时长”：(当日总时长) / (当日活跃人数)
          (SUM(ds.duration) / 1000.0 / 60.0) / NULLIF(COUNT(DISTINCT ds.user_id), 0) AS avg_time_spent_minutes
        FROM daily_sessions ds
        GROUP BY
          ds.variation_id,
          ds.event_date
        ORDER BY
          ds.variation_id,
          ds.event_date;
    """
    params = {
        "experiment_name": experiment_name,
        "start_date": start_date,
        "end_date": end_date,
    }
    with engine.connect() as conn:
        df = conn.execute(text(query), params).fetchall()
    print(f"[COHORT-TIME-SPENT-HEATMAP] 实验 {experiment_name} 查询到 {len(df)} 条记录")
    return df
