from sqlalchemy import text
from datetime import datetime, timedelta

def fetch_active_user_retention(experiment_name, start_date, end_date, engine, day):
    """
    查询活跃用户指定 day (1/3/7/15) 留存率，每天查询，返回字段如:
    {variation, active_date, active_users, d1_retained_users, d1_retention_rate}
    """
    all_results = []
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    delta = timedelta(days=1)

    # 拼出动态字段名
    retained_users_field = f'd{day}_retained_users'
    retention_rate_field = f'd{day}_retention_rate'

    while start_dt <= end_dt:
        current_date_str = start_dt.strftime("%Y-%m-%d")
        query = f"""
        SELECT
          e.variation AS variation,
          '{current_date_str}' AS active_date,
          COUNT(DISTINCT base.user_id) AS active_users,
          COUNT(DISTINCT dN.user_id) AS {retained_users_field},
          ROUND(COUNT(DISTINCT dN.user_id) / NULLIF(COUNT(DISTINCT base.user_id), 0), 4) AS {retention_rate_field}
        FROM (
            -- 当天活跃用户
            SELECT user_id
            FROM flow_wide_info.tbl_wide_active_user_app_info
            WHERE active_date = '{current_date_str}'
              AND keep_alive_flag = 1
              AND user_id IS NOT NULL AND user_id != ''
        ) base
        LEFT JOIN (
            -- 实验分组
            SELECT user_id, CAST(variation_id AS CHAR) AS variation
            FROM (
                SELECT user_id, variation_id,
                    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp_assigned ASC) AS rn
                FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                WHERE experiment_id = '{experiment_name}'
            ) t
            WHERE rn = 1
        ) e ON base.user_id = e.user_id
        LEFT JOIN (
            -- N天后还活跃的
            SELECT user_id
            FROM flow_wide_info.tbl_wide_active_user_app_info
            WHERE active_date = DATE_ADD('{current_date_str}', INTERVAL {day} DAY)
              AND keep_alive_flag = 1
        ) dN ON base.user_id = dN.user_id
        WHERE e.variation IS NOT NULL
        GROUP BY e.variation
        ORDER BY e.variation;
        """
        with engine.connect() as conn:
            rows = conn.execute(text(query)).fetchall()
        for row in rows:
            # 处理字段名动态返回
            row_dict = dict(row) if not hasattr(row, '_asdict') else row._asdict()
            # 强制补充日期（防止不同SQL方言遗漏）
            row_dict['active_date'] = current_date_str
            all_results.append(row_dict)
        start_dt += delta

    print(f"RETENTION: {experiment_name} D{day} 多天合并共 {len(all_results)} 条")
    return all_results
