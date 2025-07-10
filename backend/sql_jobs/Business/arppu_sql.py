from sqlalchemy import text

def fetch_group_arppu_samples(experiment_name, start_date, end_date, engine):
    query = f'''
    WITH 
        exp AS (
            SELECT user_id, variation_id
            FROM (
                SELECT
                    user_id,
                    variation_id,
                    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_date DESC) AS rn
                FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                WHERE experiment_id = '{experiment_name}'
            ) t
            WHERE rn = 1
        ),
        active AS (
            SELECT e.variation_id, c.event_date, COUNT(DISTINCT c.user_id) AS active_users
            FROM (
                SELECT COALESCE(s.user_id, o.user_id) AS user_id,
                       COALESCE(s.event_date, o.event_date) AS event_date
                FROM flow_event_info.tbl_app_event_subscribe s
                FULL OUTER JOIN flow_event_info.tbl_app_event_currency_purchase o
                ON s.user_id = o.user_id AND s.event_date = o.event_date
                WHERE COALESCE(s.event_date, o.event_date) BETWEEN '{start_date}' AND '{end_date}'
            ) c
            JOIN exp e ON c.user_id = e.user_id
            GROUP BY e.variation_id, c.event_date
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
        combined AS (
            SELECT COALESCE(s.user_id, o.user_id) AS user_id,
                   COALESCE(s.event_date, o.event_date) AS event_date,
                   COALESCE(s.sub_revenue, 0) AS sub_revenue,
                   COALESCE(o.order_revenue, 0) AS order_revenue,
                   COALESCE(s.sub_revenue, 0) + COALESCE(o.order_revenue, 0) AS total_revenue
            FROM sub s
            FULL OUTER JOIN ord o ON s.user_id = o.user_id AND s.event_date = o.event_date
        ),
        merged AS (
            SELECT e.variation_id, c.event_date, c.user_id,
                   SUM(c.sub_revenue) AS sub_revenue,
                   SUM(c.order_revenue) AS order_revenue,
                   SUM(c.total_revenue) AS total_revenue
            FROM combined c
            JOIN exp e ON c.user_id = e.user_id
            GROUP BY e.variation_id, c.event_date, c.user_id
        )
    SELECT 
        m.variation_id,
        m.event_date,
        SUM(m.sub_revenue) AS total_subscribe_revenue,
        SUM(m.order_revenue) AS total_order_revenue,
        SUM(m.total_revenue) AS total_revenue,
        COUNT(DISTINCT CASE WHEN m.total_revenue > 0 THEN m.user_id END) AS paying_users,
        MAX(a.active_users) AS active_users,
        ROUND(
            SUM(m.total_revenue) / NULLIF(COUNT(DISTINCT CASE WHEN m.total_revenue > 0 THEN m.user_id END), 0),
        4) AS arppu
    FROM merged m
    LEFT JOIN active a ON m.variation_id = a.variation_id AND m.event_date = a.event_date
    GROUP BY m.variation_id, m.event_date
    ORDER BY m.event_date ASC, m.variation_id ASC;
    '''
    with engine.connect() as conn:
        df = conn.execute(text(query)).fetchall()
    print(f"ARPPU: 实验 {experiment_name} 查询到 {len(df)} 条记录")
    return df
# 后端返回的数据结构（建议 print 出来一条看看）
    print(df[:2])


