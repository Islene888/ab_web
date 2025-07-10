from sqlalchemy import text
from datetime import datetime, timedelta

def fetch_group_payment_rate_samples(experiment_name, start_date, end_date, engine):
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
              AND event_date = '{current_date_str}'
          ) t
          WHERE rn = 1
        ),
        first_active AS (
          SELECT user_id, variation_id, MIN(event_date) AS first_active_date
          FROM exp
          GROUP BY user_id, variation_id
        ),
        act_raw AS (
          SELECT DISTINCT
            e.user_id,
            e.variation_id,
            p.event_date
          FROM exp e
          JOIN flow_event_info.tbl_app_event_all_purchase p
            ON e.user_id = p.user_id
           AND p.event_date = '{current_date_str}'
        ),
        purchase AS (
          SELECT
            e.user_id,
            e.variation_id,
            p.event_date,
            p.revenue,
            DATEDIFF(p.event_date, f.first_active_date) AS day_diff
          FROM exp e
          JOIN first_active f ON e.user_id = f.user_id AND e.variation_id = f.variation_id
          JOIN flow_event_info.tbl_app_event_all_purchase p
            ON e.user_id = p.user_id
           AND p.type IN ('subscription', 'currency')
           AND p.event_date = '{current_date_str}'
        ),
        final_purchase AS (
          SELECT DISTINCT user_id, variation_id, event_date, revenue, day_diff
          FROM purchase
        ),
        final_activity AS (
          SELECT DISTINCT user_id, variation_id, event_date
          FROM act_raw
        )
        SELECT 
          a.variation_id,
          a.event_date,
          COUNT(DISTINCT p.user_id) AS paying_users,
          COUNT(DISTINCT a.user_id) AS all_users，
          SUM(IFNULL(p.revenue, 0)) AS revenue,
          ROUND(SUM(CASE WHEN p.day_diff <= 7 THEN p.revenue ELSE 0 END) / NULLIF(COUNT(DISTINCT p.user_id), 0), 4) AS LTV7,
          ROUND(SUM(IFNULL(p.revenue, 0)) / NULLIF(COUNT(DISTINCT p.user_id), 0), 4) AS LTV_experiment,
          COUNT(DISTINCT a.user_id) AS total_users,
          ROUND(COUNT(DISTINCT p.user_id) / NULLIF(COUNT(DISTINCT a.user_id), 0), 4) AS purchase_rate
        FROM final_activity a
        LEFT JOIN final_purchase p
          ON a.user_id = p.user_id AND a.variation_id = p.variation_id AND a.event_date = p.event_date
        GROUP BY a.variation_id, a.event_date; 
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