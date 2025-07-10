from sqlalchemy import text

def fetch_group_payment_rate_new_samples(experiment_name, start_date, end_date, engine):
    # 只用 start_date 作为 event_date_str，end_date 可忽略或用于多天循环
    event_date_str = start_date
    day3_date = f"DATE_ADD('{start_date}', INTERVAL 3 DAY)"
    query = f'''
    WITH 
        cohort AS (
          SELECT
            a.user_id,
            a.variation_id,
            a.event_date,
            ROW_NUMBER() OVER (PARTITION BY a.user_id, a.variation_id, a.event_date) AS rn
          FROM flow_wide_info.tbl_wide_experiment_assignment_hi a
          WHERE a.experiment_id = '{experiment_name}'
            AND a.event_date BETWEEN '{start_date}' AND '{end_date}'
        ),
        dnu AS (
          SELECT
            t.user_id,
            c.variation_id,
            t.event_date
          FROM flow_report_app.tbl_active_retention_new_page_view t
          JOIN cohort c ON t.user_id = c.user_id AND t.event_date = c.event_date
          WHERE c.rn = 1
        ),
        pay_user AS (
          SELECT
            c.user_id,
            c.variation_id,
            c.event_date
          FROM dnu c
          JOIN flow_event_info.tbl_app_event_all_purchase p
            ON c.user_id = p.user_id AND c.event_date = p.event_date
          WHERE p.type IN ('subscription', 'currency')
            AND p.event_date BETWEEN '{start_date}' AND DATE_ADD('{start_date}', INTERVAL 3 DAY)
        )
    SELECT
      d.event_date,
      d.variation_id,
      COUNT(DISTINCT d.user_id) AS dnu,
      COUNT(DISTINCT p.user_id) AS pay_user_day1,
      ROUND(COUNT(DISTINCT p.user_id) / NULLIF(COUNT(DISTINCT d.user_id),0), 4) AS pay_rate_day1
    FROM dnu d
    LEFT JOIN pay_user p
      ON d.user_id = p.user_id AND d.variation_id = p.variation_id AND d.event_date = p.event_date
    GROUP BY d.event_date, d.variation_id
    ORDER BY d.event_date DESC, d.variation_id;
    '''
    with engine.connect() as conn:
        df = conn.execute(text(query)).fetchall()
    print(f"Payment Rate New: 实验 {experiment_name} 查询到 {len(df)} 条记录")
    return df 