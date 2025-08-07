from sqlalchemy import text


def fetch_cohort_retention_heatmap(experiment_name, start_date, end_date, engine, max_days=30):
    """
    【"定点快照"群组逻辑版】
    1. 将在 start_date 当天活跃的用户定义为一个固定的群组 (Cohort)。
    2. 追踪这个群组从 start_date 到 end_date 每一天的留存情况。
    """
    # max_days 参数在这里不再需要，因为周期由 end_date 决定，但保留以兼容接口

    # 使用参数化查询 :key 来保证安全
    query = """
    WITH
      -- 步骤1: 定义我们的“定点群组”。
      -- 即，在指定的 start_date 当天，所有活跃过的实验用户。
      point_in_time_cohort AS (
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
      -- 步骤2: 计算这个“定点群组”的初始总人数，这将是留存率的固定分母
      cohort_initial_size AS (
        SELECT
          variation_id,
          COUNT(DISTINCT user_id) AS new_users
        FROM point_in_time_cohort
        GROUP BY variation_id
      ),
      -- 步骤3: 计算这个“定点群组”在之后每一天的活跃人数 (留存人数)
      daily_retained_counts AS (
        SELECT
          pic.variation_id,
          a.active_date,
          COUNT(DISTINCT pic.user_id) AS retained_users
        FROM point_in_time_cohort pic
        JOIN flow_wide_info.tbl_wide_active_user_app_info a ON pic.user_id = a.user_id
        -- 追踪周期是从 start_date 到 end_date
        WHERE a.active_date BETWEEN :start_date AND :end_date
        GROUP BY pic.variation_id, a.active_date
      )
    -- 步骤4: 最终聚合输出结果
    SELECT
      drc.variation_id,
      -- “注册日期”在这里作为一个标签，其值就是我们定点群组的基准日 start_date
      :start_date AS register_date,
      -- 计算今天是追踪周期的第几天 (Day 0, Day 1, ...)
      DATEDIFF(drc.active_date, :start_date) AS cohort_day,
      cs.new_users, -- 分母：群组初始总人数
      drc.retained_users, -- 分子：当天活跃的群组内用户数
      -- 计算留存率
      ROUND(drc.retained_users * 1.0 / NULLIF(cs.new_users, 0), 4) AS retention_rate
    FROM daily_retained_counts drc
    JOIN cohort_initial_size cs ON drc.variation_id = cs.variation_id
    ORDER BY drc.variation_id, drc.active_date;
    """

    params = {
        "experiment_name": experiment_name,
        "start_date": start_date,
        "end_date": end_date
    }

    with engine.connect() as conn:
        result_proxy = conn.execute(text(query), params)
        all_results = [dict(row._mapping) for row in result_proxy]

    print(f"[COHORT-RETENTION-HEATMAP - 定点快照模式] 实验 {experiment_name} 查询到 {len(all_results)} 条记录")
    return all_results