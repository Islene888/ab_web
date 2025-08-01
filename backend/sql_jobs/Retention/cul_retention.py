from sqlalchemy import text
from datetime import datetime, timedelta

def fetch_group_cumulative_retained_users_daily(experiment_name, start_date, end_date, engine):
    """
    每天累计留存用户数和累计注册用户数、留存率，分variation输出
    """
    all_results = []
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    delta = timedelta(days=1)
    current_dt = start_dt

    while current_dt <= end_dt:
        current_date_str = current_dt.strftime("%Y-%m-%d")
        # 累计注册用户分variation
        query = f"""
        WITH
        first_active AS (
            SELECT
                e.user_id,
                e.variation_id,
                MIN(a.active_date) AS first_active_date
            FROM flow_wide_info.tbl_wide_active_user_app_info a
            JOIN (
                SELECT user_id, variation_id
                FROM (
                    SELECT user_id, variation_id,
                        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp_assigned ASC) AS rn
                    FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                    WHERE experiment_id = '{experiment_name}'
                ) t WHERE rn = 1
            ) e ON a.user_id = e.user_id
            WHERE a.active_date <= '{current_date_str}'
            GROUP BY e.user_id, e.variation_id
        ),
        cumulative_registered AS (
            SELECT variation_id, COUNT(DISTINCT user_id) AS cumulative_registered_users
            FROM first_active
            GROUP BY variation_id
        ),
        retained_users AS (
            SELECT
                f.variation_id,
                f.user_id
            FROM first_active f
            JOIN flow_wide_info.tbl_wide_active_user_app_info a2
                ON f.user_id = a2.user_id
                AND a2.active_date > f.first_active_date
                AND a2.active_date <= '{current_date_str}'
            GROUP BY f.variation_id, f.user_id
        ),
        cumulative_retained AS (
            SELECT variation_id, COUNT(DISTINCT user_id) AS cumulative_retained_users
            FROM retained_users
            GROUP BY variation_id
        )
        SELECT
            '{current_date_str}' AS event_date,
            r.variation_id,
            COALESCE(cr.cumulative_retained_users, 0) AS cumulative_retained_users,
            COALESCE(reg.cumulative_registered_users, 0) AS cumulative_registered_users,
            ROUND(COALESCE(cr.cumulative_retained_users, 0) / NULLIF(reg.cumulative_registered_users, 0), 4) AS cumulative_retention_rate
        FROM
            (SELECT variation_id FROM cumulative_registered
             UNION
             SELECT variation_id FROM cumulative_retained) r
        LEFT JOIN cumulative_retained cr ON r.variation_id = cr.variation_id
        LEFT JOIN cumulative_registered reg ON r.variation_id = reg.variation_id
        ORDER BY r.variation_id;
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

    print(f"Cumulative Retained Users (每日): 实验 {experiment_name} 多天累计查询到 {len(all_results)} 条记录")
    return all_results
