import logging
from sqlalchemy import text

def fetch_group_arpu_samples(experiment_name, start_date, end_date, engine):
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
                    AND event_date BETWEEN '{start_date}' AND '{end_date}'
            ) t
            WHERE rn = 1
        ),
        daily_active AS (
            SELECT
                e.event_date,
                e.variation_id,
                COUNT(DISTINCT pv.user_id) AS active_users
            FROM flow_event_info.tbl_app_session_info pv
            JOIN exp e ON pv.user_id = e.user_id AND pv.event_date = e.event_date
            GROUP BY e.event_date, e.variation_id
        ),
        sub AS (
            SELECT user_id, event_date, SUM(revenue) AS sub_revenue
            FROM flow_event_info.tbl_app_event_subscribe
            WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY user_id, event_date
        ),
        ord AS (
            SELECT user_id, event_date, SUM(revenue) AS order_revenue
            FROM flow_event_info.tbl_app_event_currency_purchase
            WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY user_id, event_date
        ),
        user_revenue AS (
            SELECT
                e.event_date,
                e.variation_id,
                COALESCE(s.sub_revenue, 0) + COALESCE(o.order_revenue, 0) AS total_revenue
            FROM exp e
            LEFT JOIN sub s ON e.user_id = s.user_id AND e.event_date = s.event_date
            LEFT JOIN ord o ON e.user_id = o.user_id AND e.event_date = o.event_date
        ),
        group_revenue AS (
            SELECT
                event_date,
                variation_id,
                SUM(total_revenue) AS revenue
            FROM user_revenue
            GROUP BY event_date, variation_id
        ),
        daily_ad AS (
            SELECT event_date, SUM(ad_revenue) AS ad_revenue
            FROM flow_event_info.tbl_app_event_ads_impression
            WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY event_date
        ),
        daily_total_active AS (
            SELECT event_date, SUM(active_users) AS total_active
            FROM daily_active
            GROUP BY event_date
        )
    SELECT
        da.variation_id,
        da.event_date,
        COALESCE(gr.revenue, 0)
            + COALESCE(dad.ad_revenue, 0) * da.active_users / NULLIF(dta.total_active, 0)
            AS total_revenue,
        da.active_users,
        ROUND(
            (
                COALESCE(gr.revenue, 0)
                + COALESCE(dad.ad_revenue, 0) * da.active_users / NULLIF(dta.total_active, 0)
            ) / NULLIF(da.active_users, 0),
        4) AS arpu
    FROM daily_active da
    LEFT JOIN group_revenue gr
        ON da.event_date = gr.event_date AND da.variation_id = gr.variation_id
    LEFT JOIN daily_ad dad
        ON da.event_date = dad.event_date
    LEFT JOIN daily_total_active dta
        ON da.event_date = dta.event_date
    WHERE da.event_date >= '{start_date}' AND da.event_date <= '{end_date}';
    '''
    with engine.connect() as conn:
        df = conn.execute(text(query)).fetchall()
    print(f"ARPU: 实验 {experiment_name} 查询到 {len(df)} 条记录")
    return df 