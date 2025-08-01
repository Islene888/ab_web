from sqlalchemy import text
from datetime import datetime, timedelta

def fetch_group_cumulative_ltv_daily(experiment_name, start_date, end_date, engine):
    """
    按天查询累计 LTV（日累计），**每一天都是cohort视角**，所有分配到实验的用户，从分配日累计到该天的所有收入。
    返回：每一天、每组的 cumulative_ltv
    """
    all_results = []
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    delta = timedelta(days=1)
    current_dt = start_dt

    while current_dt <= end_dt:
        current_date_str = current_dt.strftime("%Y-%m-%d")
        # --- SQL 核心 ---
        query = f"""
        WITH assignment AS (
            SELECT user_id, variation_id, MIN(event_date) AS assigned_date
            FROM flow_wide_info.tbl_wide_experiment_assignment_hi
            WHERE experiment_id = '{experiment_name}'
            GROUP BY user_id, variation_id
        ),
        cohort AS (
            SELECT user_id, variation_id, assigned_date
            FROM assignment
            WHERE assigned_date <= '{current_date_str}'
        ),
        subscribe AS (
            SELECT user_id, event_date, SUM(revenue) AS revenue
            FROM flow_event_info.tbl_app_event_subscribe
            WHERE event_date <= '{current_date_str}'
            GROUP BY user_id, event_date
        ),
        order_purchase AS (
            SELECT user_id, event_date, SUM(revenue) AS revenue
            FROM flow_event_info.tbl_app_event_currency_purchase
            WHERE event_date <= '{current_date_str}'
            GROUP BY user_id, event_date
        ),
        ads AS (
            SELECT user_id, event_date, SUM(ad_revenue) AS revenue
            FROM flow_event_info.tbl_app_event_ads_impression
            WHERE event_date <= '{current_date_str}'
            GROUP BY user_id, event_date
        ),
        user_revenue AS (
            SELECT
                c.user_id,
                c.variation_id,
                -- 计算该用户从assigned_date ~ 当前日的所有收入
                SUM(COALESCE(s.revenue, 0) + COALESCE(o.revenue, 0) + COALESCE(a.revenue, 0)) AS total_revenue
            FROM cohort c
            LEFT JOIN subscribe s ON c.user_id = s.user_id AND s.event_date >= c.assigned_date AND s.event_date <= '{current_date_str}'
            LEFT JOIN order_purchase o ON c.user_id = o.user_id AND o.event_date >= c.assigned_date AND o.event_date <= '{current_date_str}'
            LEFT JOIN ads a ON c.user_id = a.user_id AND a.event_date >= c.assigned_date AND a.event_date <= '{current_date_str}'
            GROUP BY c.user_id, c.variation_id
        )
        SELECT
            '{current_date_str}' AS event_date,
            variation_id,
            SUM(total_revenue) AS cumulative_revenue,
            COUNT(DISTINCT user_id) AS cumulative_users,
            ROUND(SUM(total_revenue) / NULLIF(COUNT(DISTINCT user_id),0), 4) AS cumulative_ltv
        FROM user_revenue
        GROUP BY variation_id
        ;
        """

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

    print(f"Cumulative LTV (每日递增): 实验 {experiment_name} 多天累计查询到 {len(all_results)} 条记录")
    return all_results
