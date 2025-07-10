import logging
from sqlalchemy import text

def fetch_group_aov_samples(experiment_name, start_date, end_date, engine):
    query = f'''
    SELECT
      variation_id,
      event_date,
      SUM(revenue) AS total_revenue,
      COUNT(*) AS total_order_cnt,
      ROUND(SUM(revenue) * 1.0 / NULLIF(COUNT(*), 0), 2) AS aov
    FROM (
      SELECT
        eu.variation_id,
        o.event_date,
        o.revenue
      FROM flow_event_info.tbl_app_event_currency_purchase o
      JOIN (
        SELECT
          user_id,
          CAST(variation_id AS CHAR) AS variation_id,
          MIN(timestamp_assigned) AS timestamp_assigned
        FROM flow_wide_info.tbl_wide_experiment_assignment_hi
        WHERE experiment_id = '{experiment_name}'
          AND timestamp_assigned BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'
        GROUP BY user_id, variation_id
      ) eu ON o.user_id = eu.user_id
      WHERE o.event_date BETWEEN '{start_date}' AND '{end_date}'
        AND o.event_date >= DATE(eu.timestamp_assigned)
      UNION ALL
      SELECT
        eu.variation_id,
        o.event_date,
        o.revenue
      FROM flow_event_info.tbl_app_event_subscribe o
      JOIN (
        SELECT
          user_id,
          CAST(variation_id AS CHAR) AS variation_id,
          MIN(timestamp_assigned) AS timestamp_assigned
        FROM flow_wide_info.tbl_wide_experiment_assignment_hi
        WHERE experiment_id = '{experiment_name}'
          AND timestamp_assigned BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'
        GROUP BY user_id, variation_id
      ) eu ON o.user_id = eu.user_id
      WHERE o.event_date BETWEEN '{start_date}' AND '{end_date}'
        AND o.event_date >= DATE(eu.timestamp_assigned)
    ) t
    GROUP BY variation_id, event_date
    ORDER BY variation_id, event_date;
    '''
    with engine.connect() as conn:
        df = conn.execute(text(query)).fetchall()
    print(f"AOV: 实验 {experiment_name} 查询到 {len(df)} 条记录")
    return df 