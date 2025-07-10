from sqlalchemy import text
from datetime import datetime, timedelta

def fetch_group_cancel_sub_samples(experiment_name, start_date, end_date, engine):
    """
    多天合并查询，返回[start_date, end_date]区间内所有天的分组 chat round 数据，结构与 AOV 一致。
    """
    all_results = []
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    delta = timedelta(days=1)
    current_dt = start_dt

    while current_dt <= end_dt:
        current_date_str = current_dt.strftime("%Y-%m-%d")
        # 构造 SQL，传入 current_date
        query = f'''
WITH union_events AS (
    SELECT user_id, order_id, country, DATE(sub_date) AS sub_date, notification_type, 'apple' AS store_type
    FROM flow_wide_info.tbl_wide_business_subscribe_apple_detail
    UNION ALL
    SELECT user_id, order_id, country, DATE(sub_date) AS sub_date, notification_type, 'google' AS store_type
    FROM flow_wide_info.tbl_wide_business_subscribe_google_detail
),
exp_group AS (
  SELECT user_id, event_date, variation_id
  FROM (
    SELECT
      user_id,
      event_date,
      variation_id,
      ROW_NUMBER() OVER (PARTITION BY user_id, event_date ORDER BY event_date DESC) AS rn
    FROM flow_wide_info.tbl_wide_experiment_assignment_hi
    WHERE experiment_id = '{experiment_name}'
  ) t
  WHERE rn = 1
),
active_users AS (
    SELECT 
        a.event_date,
        a.user_id,
        a.country
    FROM flow_event_info.tbl_wide_user_active_geo_daily a
    WHERE a.event_date = '{current_date_str}'
),
new_subs AS (
  SELECT
    e.user_id,
    e.order_id,
    au.country,
    e.sub_date,
    e.store_type,
    g.variation_id
  FROM union_events e
  LEFT JOIN exp_group g
    ON e.user_id = g.user_id AND e.sub_date = g.event_date
  LEFT JOIN active_users au
    ON e.user_id = au.user_id AND e.sub_date = au.event_date
  WHERE 
    (
      (e.store_type = 'apple' AND e.notification_type IN ('SUBSCRIBED', 'DID_RENEW'))
      OR
      (e.store_type = 'google' AND e.notification_type IN ('2', '4'))
    )
    AND e.sub_date = '{current_date_str}'
),
cancel AS (
  SELECT 
    c.user_id, 
    c.order_id, 
    au.country AS country, 
    c.sub_date AS cancel_date, 
    c.store_type
  FROM union_events c
  LEFT JOIN active_users au
    ON c.user_id = au.user_id AND c.sub_date = au.event_date
  WHERE 
    (
      (c.store_type = 'apple' AND c.notification_type IN ('DID_CHANGE_RENEWAL_STATUS'))
      OR
      (c.store_type = 'google' AND c.notification_type = '3')
    )
)
SELECT
  '{current_date_str}' AS event_date,
  n.country,
  n.variation_id,
  COUNT(DISTINCT n.user_id) AS total_subs,
  COUNT(DISTINCT CASE WHEN c.cancel_date IS NOT NULL AND c.cancel_date <= DATE_ADD(n.sub_date, INTERVAL 3 DAY) THEN n.user_id END) AS unsub_in3d,
  ROUND(
    COUNT(DISTINCT CASE WHEN c.cancel_date IS NOT NULL AND c.cancel_date <= DATE_ADD(n.sub_date, INTERVAL 3 DAY) THEN n.user_id END)
    / NULLIF(COUNT(DISTINCT n.user_id), 0), 4
  ) AS unsub_rate_day3
FROM new_subs n
LEFT JOIN cancel c
  ON n.user_id = c.user_id AND n.order_id = c.order_id AND n.country = c.country
    AND c.cancel_date >= n.sub_date AND c.cancel_date <= DATE_ADD(n.sub_date, INTERVAL 3 DAY)
WHERE n.variation_id IS NOT NULL
GROUP BY n.country, n.variation_id
ORDER BY n.country, n.variation_id
'''
        with engine.connect() as conn:
            day_result = conn.execute(text(query)).fetchall()
        # day_result 可能是 Row/tuple，转 dict
        for row in day_result:
            if hasattr(row, '_asdict'):
                row_dict = row._asdict()
            else:
                row_dict = dict(row)
            all_results.append(row_dict)
        current_dt += delta

    print(f"CLICK: 实验 {experiment_name} 多天合并查询到 {len(all_results)} 条记录")
    return all_results