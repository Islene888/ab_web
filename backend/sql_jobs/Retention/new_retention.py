from sqlalchemy import text
from datetime import datetime, timedelta


def fetch_new_user_retention(experiment_name, start_date, end_date, engine, day):
    """
    【修改版】查询新用户指定 day (1/3/7/15) 留存率。
    代码结构参考 fetch_active_user_retention，采用 Python 循环每日查询。
    """
    all_results = []
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    delta = timedelta(days=1)

    # ✅ 1. 像范例一样，动态拼接字段名
    retained_users_field = f'd{day}_retained_users'
    retention_rate_field = f'd{day}_retention_rate'

    # ✅ 2. 像范例一样，使用 Python 的 while 循环来处理每一天
    while start_dt <= end_dt:
        current_date_str = start_dt.strftime("%Y-%m-%d")

        # ✅ 3. SQL 查询逻辑被修改为只计算一天的留存
        query = f"""
        SELECT
          e.variation AS variation,
          '{current_date_str}' AS first_visit_date,
          COUNT(DISTINCT u.user_id) AS new_users,
          COUNT(DISTINCT a.user_id) AS {retained_users_field},
          ROUND(COUNT(DISTINCT a.user_id) / NULLIF(COUNT(DISTINCT u.user_id), 0), 4) AS {retention_rate_field}
        FROM (
            -- 当天的新增用户
            SELECT user_id
            FROM flow_wide_info.tbl_wide_user_first_visit_app_info
            WHERE DATE(first_visit_date) = '{current_date_str}'
              AND user_id IS NOT NULL AND user_id != ''
        ) u
        LEFT JOIN (
            -- 实验分组 (逻辑不变)
            SELECT user_id, CAST(variation_id AS CHAR) AS variation
            FROM (
                SELECT user_id, variation_id,
                    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp_assigned ASC) AS rn
                FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                WHERE experiment_id = '{experiment_name}'
            ) t
            WHERE rn = 1
        ) e ON u.user_id = e.user_id
        LEFT JOIN (
            -- N天后还活跃的
            SELECT user_id
            FROM flow_wide_info.tbl_wide_active_user_app_info
            WHERE active_date = DATE_ADD('{current_date_str}', INTERVAL {day} DAY)
              AND keep_alive_flag = 1
        ) a ON u.user_id = a.user_id
        WHERE e.variation IS NOT NULL
        GROUP BY e.variation
        ORDER BY e.variation;
        """
        with engine.connect() as conn:
            rows = conn.execute(text(query)).fetchall()

        for row in rows:
            # ✅ 4. 结果处理逻辑与范例保持一致
            row_dict = dict(row) if not hasattr(row, '_asdict') else row._asdict()
            row_dict['first_visit_date'] = current_date_str
            all_results.append(row_dict)

        start_dt += delta

    print(f"NEW RETENTION: {experiment_name} D{day} 多天合并共 {len(all_results)} 条")
    return all_results