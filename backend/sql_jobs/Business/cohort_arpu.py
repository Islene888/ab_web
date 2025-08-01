from sqlalchemy import text

def fetch_cohort_arpu_heatmap(experiment_name, start_date, end_date, engine):
    """
    返回结构：[
      {
        "variation_id": ...,
        "register_date": ...,
        "cohort_day": ...,
        "arpu": ...,
        "total_revenue": ...,
        "active_users": ...,
      }, ...
    ]
    """
    query = f"""
    WITH
      -- 找到所有 cohort 用户的注册日/首活日
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
      -- 找到所有活跃事件，匹配cohort
      user_events AS (
        SELECT
          fa.variation_id,
          fa.register_date,
          se.event_date,
          DATEDIFF(se.event_date, fa.register_date) AS cohort_day,
          se.user_id
        FROM first_active fa
        JOIN flow_event_info.tbl_app_session_info se
          ON fa.user_id = se.user_id
         AND se.event_date BETWEEN '{start_date}' AND '{end_date}'
         AND se.event_date >= fa.register_date
         AND DATEDIFF(se.event_date, fa.register_date) BETWEEN 0 AND 29  -- 最多看30天
      ),
      -- 收入表 (subscribe + order + 广告)
      user_revenue AS (
        SELECT
          fa.user_id,
          fa.variation_id,
          fa.register_date,
          e.event_date,
          DATEDIFF(e.event_date, fa.register_date) AS cohort_day,
          COALESCE(s.sub_revenue, 0) + COALESCE(o.order_revenue, 0) + COALESCE(a.ad_revenue, 0) AS total_revenue
        FROM first_active fa
        JOIN flow_event_info.tbl_app_session_info e
          ON fa.user_id = e.user_id AND e.event_date >= fa.register_date AND e.event_date BETWEEN '{start_date}' AND '{end_date}'
        LEFT JOIN (
          SELECT user_id, event_date, SUM(revenue) AS sub_revenue
          FROM flow_event_info.tbl_app_event_subscribe
          WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
          GROUP BY user_id, event_date
        ) s ON fa.user_id = s.user_id AND e.event_date = s.event_date
        LEFT JOIN (
          SELECT user_id, event_date, SUM(revenue) AS order_revenue
          FROM flow_event_info.tbl_app_event_currency_purchase
          WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
          GROUP BY user_id, event_date
        ) o ON fa.user_id = o.user_id AND e.event_date = o.event_date
        LEFT JOIN (
          SELECT user_id, event_date, SUM(ad_revenue) AS ad_revenue
          FROM flow_event_info.tbl_app_event_ads_impression
          WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
          GROUP BY user_id, event_date
        ) a ON fa.user_id = a.user_id AND e.event_date = a.event_date
      )
    -- 聚合输出 cohort 表
    SELECT
      ur.variation_id,
      ur.register_date,
      ur.cohort_day,
      SUM(ur.total_revenue) AS total_revenue,
      COUNT(DISTINCT ur.user_id) AS active_users,
      ROUND(SUM(ur.total_revenue) / NULLIF(COUNT(DISTINCT ur.user_id), 0), 4) AS arpu
    FROM user_revenue ur
    GROUP BY ur.variation_id, ur.register_date, ur.cohort_day
    ORDER BY ur.variation_id, ur.register_date, ur.cohort_day
    ;
    """
    with engine.connect() as conn:
        df = conn.execute(text(query)).fetchall()
    print(f"[COHORT-ARPU-HEATMAP] 实验 {experiment_name} 查询到 {len(df)} 条记录")
    return df
