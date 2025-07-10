from sqlalchemy import text
from datetime import datetime, timedelta

def fetch_group_subscribe_new_samples(experiment_name, start_date, end_date, engine):
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
    new_users AS (
        SELECT user_id
        FROM flow_wide_info.tbl_wide_user_first_visit_app_info
        WHERE DATE(first_visit_date) = '{current_date_str}'
    ),
    experiment_users AS (
        SELECT t.user_id, t.variation_id
        FROM (
            SELECT
                user_id,
                variation_id,
                event_date,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_date DESC) AS rn
            FROM flow_wide_info.tbl_wide_experiment_assignment_hi
            WHERE experiment_id = '{experiment_name}'
              AND event_date = '{current_date_str}'
        ) t
        WHERE rn = 1
    ),
    new_exp_users AS (
        SELECT e.user_id, e.variation_id
        FROM experiment_users e
        INNER JOIN new_users n ON e.user_id = n.user_id
    ),
    subscribe_orders_with_new_users AS (
        SELECT
            n.variation_id,
            o.event_date,
            o.revenue
        FROM new_exp_users n
        JOIN flow_event_info.tbl_app_event_subscribe o
            ON n.user_id = o.user_id
        WHERE o.event_date >= '{current_date_str}'
          AND o.event_date <= DATE_ADD('{current_date_str}', INTERVAL 3 DAY)
    )
    SELECT
      '{current_date_str}' AS event_date,
      variation_id,
      -- day1
      SUM(CASE WHEN event_date <= DATE_ADD('{current_date_str}', INTERVAL 1 DAY) THEN revenue ELSE 0 END) AS subscribe_revenue_day1,
      COUNT(CASE WHEN event_date <= DATE_ADD('{current_date_str}', INTERVAL 1 DAY) THEN 1 END) AS subscribe_order_cnt_day1,
      ROUND(
        SUM(CASE WHEN event_date <= DATE_ADD('{current_date_str}', INTERVAL 1 DAY) THEN revenue ELSE 0 END)
        / NULLIF(COUNT(CASE WHEN event_date <= DATE_ADD('{current_date_str}', INTERVAL 1 DAY) THEN 1 END), 0), 2
      ) AS aov_subscribe_day1,
      -- day3
      SUM(revenue) AS subscribe_revenue_day3,
      COUNT(*) AS subscribe_order_cnt_day3,
      ROUND(SUM(revenue) / NULLIF(COUNT(*), 0), 2) AS aov_subscribe_day3
    FROM subscribe_orders_with_new_users
    GROUP BY variation_id;
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