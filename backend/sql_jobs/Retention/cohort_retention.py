from sqlalchemy import text

def fetch_cohort_retention_heatmap(experiment_name, start_date, end_date, engine, max_days=30):
    """
    查询 cohort 留存率热力图
    :param experiment_name: 实验名称
    :param start_date: 注册起始日
    :param end_date: 注册结束日
    :param max_days: 统计多少天内的留存，默认30
    返回结构: [
        {
            "variation_id": ...,
            "register_date": ...,
            "cohort_day": ...,
            "retained_users": ...,
            "new_users": ...,
            "retention_rate": ...
        }, ...
    ]
    """
    query = f"""
    WITH
      -- 找到cohort用户的注册日（首活日）
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
      -- 每个cohort用户注册后0~N天内，每天是否还活跃
      cohort_active AS (
        SELECT
          fa.variation_id,
          fa.register_date,
          DATEDIFF(b.active_date, fa.register_date) AS cohort_day,
          fa.user_id
        FROM first_active fa
        JOIN flow_wide_info.tbl_wide_active_user_app_info b
          ON fa.user_id = b.user_id
         AND b.active_date BETWEEN fa.register_date AND DATE_ADD(fa.register_date, INTERVAL {max_days} DAY)
         AND DATEDIFF(b.active_date, fa.register_date) BETWEEN 0 AND {max_days}
      )
    SELECT
      ca.variation_id,
      ca.register_date,
      ca.cohort_day,
      COUNT(DISTINCT CASE WHEN ca.cohort_day = 0 THEN ca.user_id END) AS new_users,
      COUNT(DISTINCT ca.user_id) AS retained_users,
      -- 留存率 = 第N天还在的/注册日新用户数
      ROUND(COUNT(DISTINCT ca.user_id) / NULLIF(COUNT(DISTINCT CASE WHEN ca.cohort_day = 0 THEN ca.user_id END), 0), 4) AS retention_rate
    FROM cohort_active ca
    GROUP BY ca.variation_id, ca.register_date, ca.cohort_day
    ORDER BY ca.variation_id, ca.register_date, ca.cohort_day
    ;
    """
    with engine.connect() as conn:
        df = conn.execute(text(query)).fetchall()
    print(f"[COHORT-RETENTION-HEATMAP] 实验 {experiment_name} 查询到 {len(df)} 条记录")
    return df
