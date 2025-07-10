from sqlalchemy import text

def fetch_group_payment_rate_all_samples(experiment_name, start_date, end_date, engine):
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
        active_users AS (
            SELECT 
                a.event_date,
                a.user_id
            FROM flow_event_info.tbl_app_session_info  a
            WHERE a.event_date BETWEEN '{start_date}' AND '{end_date}'
        ),
        exp_active AS (
            SELECT 
                e.event_date,
                e.variation_id,
                e.user_id
            FROM exp e
            JOIN active_users a
                ON e.user_id = a.user_id AND e.event_date = a.event_date
        ),
        pay_users AS (
            SELECT 
                p.event_date,
                p.user_id
            FROM flow_event_info.tbl_app_event_all_purchase p
            WHERE p.type IN ('subscription', 'currency')
              AND p.event_date BETWEEN '{start_date}' AND '{end_date}'
        ),
        exp_pay AS (
            SELECT
                ea.event_date,
                ea.variation_id,
                ea.user_id
            FROM exp_active ea
            JOIN pay_users pu
              ON ea.user_id = pu.user_id AND ea.event_date = pu.event_date
        )
    SELECT
        ea.event_date,
        ea.variation_id,
        COUNT(DISTINCT ea.user_id) AS active_users,
        COUNT(DISTINCT ep.user_id) AS paying_users,
        ROUND(COUNT(DISTINCT ep.user_id) / NULLIF(COUNT(DISTINCT ea.user_id), 0), 4) AS payment_rate_all
    FROM exp_active ea
    LEFT JOIN exp_pay ep
        ON ea.user_id = ep.user_id AND ea.variation_id = ep.variation_id AND ea.event_date = ep.event_date
    GROUP BY ea.event_date, ea.variation_id
    ORDER BY ea.event_date DESC, active_users DESC;
    '''
    with engine.connect() as conn:
        df = conn.execute(text(query)).fetchall()
    print(f"Payment Rate All: 实验 {experiment_name} 查询到 {len(df)} 条记录")
    return df 