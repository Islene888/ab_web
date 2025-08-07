from sqlalchemy import text
from datetime import datetime, timedelta
def fetch_group_cumulative_ltv_daily(experiment_name, start_date, end_date, engine):
    """
    查询 [start_date, end_date] 区间内，每一天、每组（variation）的累计 LTV。
    字段: event_date, variation_id, cumulative_revenue, cumulative_active_users, cumulative_ltv
    """
    query = f"""
        WITH user_variation_map AS (
            SELECT user_id, variation_id FROM (
                SELECT user_id, variation_id,
                    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_date ASC) AS rn
                FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                WHERE experiment_id = '{experiment_name}'
                  AND event_date <= '{end_date}'
            ) t WHERE rn = 1
        ),
        user_revenue_by_day AS (
            SELECT
                s.user_id,
                uvm.variation_id,
                DATE(s.event_date) AS event_date,
                COALESCE(sub.revenue, 0) + COALESCE(ord.revenue, 0) + COALESCE(ad.revenue, 0) AS revenue
            FROM (
                SELECT DISTINCT user_id, event_date FROM flow_event_info.tbl_app_session_info
                WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
            ) s
            JOIN user_variation_map uvm ON s.user_id = uvm.user_id
            LEFT JOIN (
                SELECT user_id, event_date, SUM(revenue) AS revenue
                FROM flow_event_info.tbl_app_event_subscribe
                WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY user_id, event_date
            ) sub ON s.user_id = sub.user_id AND s.event_date = sub.event_date
            LEFT JOIN (
                SELECT user_id, event_date, SUM(revenue) AS revenue
                FROM flow_event_info.tbl_app_event_currency_purchase
                WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY user_id, event_date
            ) ord ON s.user_id = ord.user_id AND s.event_date = ord.event_date
            LEFT JOIN (
                SELECT user_id, event_date, SUM(ad_revenue) AS revenue
                FROM flow_event_info.tbl_app_event_ads_impression
                WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY user_id, event_date
            ) ad ON s.user_id = ad.user_id AND s.event_date = ad.event_date
        ),
        user_first_active AS (
            SELECT user_id, variation_id, MIN(event_date) AS first_active_date
            FROM user_revenue_by_day
            GROUP BY user_id, variation_id
        ),
        date_variation AS (
            SELECT DISTINCT event_date, variation_id FROM user_revenue_by_day
        ),
        user_cumulative_by_day AS (
            SELECT dv.event_date, dv.variation_id,
                COUNT(*) AS cumulative_active_users
            FROM date_variation dv
            JOIN user_first_active ufa
                ON ufa.variation_id = dv.variation_id
                AND ufa.first_active_date <= dv.event_date
            GROUP BY dv.event_date, dv.variation_id
        ),
        revenue_per_day AS (
            SELECT event_date, variation_id, SUM(revenue) AS revenue
            FROM user_revenue_by_day
            GROUP BY event_date, variation_id
        ),
        cumulative_revenue AS (
            SELECT event_date, variation_id,
                SUM(revenue) OVER (PARTITION BY variation_id ORDER BY event_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue
            FROM revenue_per_day
        )
        SELECT
            t.event_date,
            t.variation_id,
            t.cumulative_revenue,
            u.cumulative_active_users as cumulative_users,
            ROUND(t.cumulative_revenue / NULLIF(u.cumulative_active_users, 0), 4) AS cumulative_ltv
        FROM cumulative_revenue t
        JOIN user_cumulative_by_day u
            ON t.event_date = u.event_date AND t.variation_id = u.variation_id
        ORDER BY t.variation_id, t.event_date
        ;
    """

    with engine.connect() as conn:
        result = conn.execute(text(query))
        all_results = [dict(row._mapping) for row in result]
    print(f"Cumulative LTV (每日递增): 实验 {experiment_name} 多天累计查询到 {len(all_results)} 条记录")
    return all_results

