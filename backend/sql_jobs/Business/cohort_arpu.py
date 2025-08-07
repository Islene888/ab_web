from sqlalchemy import text

def fetch_cohort_arpu_heatmap(experiment_name, start_date, end_date, engine):
    """
    返回结构：[
      {
        "variation_id": ...,
        "register_date": ...,
        "cohort_day": ...,
        "ltv": ...,
        "total_revenue": ...,
        "active_users": ...,
      }, ...
    ]
    备注：register_date 恒等于 start_date，是 cohort 分析的“定基快照”模式
    """
    query = """
    WITH 
      -- 1. 所有 start_date 之前就进入实验的用户，作为“定基 cohort”
      experiment_users AS (
        SELECT user_id, variation_id
        FROM (
          SELECT user_id, variation_id,
                 ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp_assigned ASC) AS rn
          FROM flow_wide_info.tbl_wide_experiment_assignment_hi
          WHERE experiment_id = :experiment_name
            AND timestamp_assigned < DATE_ADD(:start_date, INTERVAL 1 DAY)
        ) t
        WHERE rn = 1
      ),
      -- 2. 每组 cohort 大小
      cohort_size AS (
        SELECT variation_id, COUNT(user_id) AS cohort_users
        FROM experiment_users
        GROUP BY variation_id
      ),
      -- 3. 这群用户在分析期内的全部收入（订阅、内购、广告）
      all_user_revenue AS (
        SELECT user_id, event_date, revenue 
        FROM flow_event_info.tbl_app_event_subscribe
        WHERE event_date BETWEEN :start_date AND :end_date
        UNION ALL
        SELECT user_id, event_date, revenue 
        FROM flow_event_info.tbl_app_event_currency_purchase
        WHERE event_date BETWEEN :start_date AND :end_date
        UNION ALL
        SELECT user_id, event_date, ad_revenue AS revenue 
        FROM flow_event_info.tbl_app_event_ads_impression
        WHERE event_date BETWEEN :start_date AND :end_date
      ),
      -- 4. 每天每组收入聚合
      revenue_per_day AS (
        SELECT
          eu.variation_id,
          r.event_date,
          SUM(r.revenue) AS day_revenue
        FROM experiment_users eu
        JOIN all_user_revenue r ON eu.user_id = r.user_id
        GROUP BY eu.variation_id, r.event_date
      ),
      -- 5. 计算 cohort_day 以及累计
      ltv_cumulative AS (
        SELECT
          variation_id,
          event_date,
          DATEDIFF(event_date, :start_date) AS cohort_day,
          SUM(day_revenue) OVER (PARTITION BY variation_id ORDER BY event_date) AS cumulative_revenue
        FROM revenue_per_day
      )
    -- 输出
    SELECT
      l.variation_id,
      :start_date AS register_date,      -- 固定起点
      l.cohort_day,
      ROUND(l.cumulative_revenue / NULLIF(cs.cohort_users, 0), 4) AS ltv,
      l.cumulative_revenue AS total_revenue,
      cs.cohort_users AS active_users
    FROM ltv_cumulative l
    JOIN cohort_size cs ON l.variation_id = cs.variation_id
    ORDER BY l.variation_id, l.cohort_day;
    """

    params = {
        "experiment_name": experiment_name,
        "start_date": start_date,
        "end_date": end_date,
    }
    with engine.connect() as conn:
        df = conn.execute(text(query), params).fetchall()
    result = [dict(row) for row in df]
    print(f"[COHORT-CUMULATIVE-LTV-HEATMAP] 实验 {experiment_name} 查询到 {len(result)} 条记录")
    return result
