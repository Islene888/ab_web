from backend.service.service import register_indicator_routes, app
from backend.sql_jobs.Business.aov_sql import fetch_group_aov_samples
from backend.sql_jobs.Business.arpu_sql import fetch_group_arpu_samples
from backend.sql_jobs.Business.arppu_sql import fetch_group_arppu_samples
from backend.sql_jobs.Business.payment_rate_all_sql import fetch_group_payment_rate_all_samples
from backend.sql_jobs.Business.payment_rate_new_sql import fetch_group_payment_rate_new_samples
from backend.sql_jobs.chat_behavior.click_ratio_sql import fetch_group_click_ratio_samples

# 导入聚合接口，自动注册路由
import backend.service.retention.aggregate_api

# 在主函数维护所有指标
INDICATOR_CONFIG = {
    "aov": {
        "fetch_func": fetch_group_aov_samples,
        "variation_field": 0,   # variation_id
        "date_field": 1,        # event_date
        "value_field": 4,       # aov
        "revenue_field": 2,     # total_revenue
        "order_field": 3        # total_order_cnt
    },
    "arpu": {
        "fetch_func": fetch_group_arpu_samples,
        "variation_field": 0,   # variation_id
        "date_field": 1,        # event_date
        "value_field": 4,
        "revenue_field": 2,
        "order_field": 3
    },
    "arppu": {
        "fetch_func": fetch_group_arppu_samples,
        "variation_field": 0,   # variation_id
        "date_field": 1,        # event_date
        "value_field": 7,       # arppu
        "revenue_field": 4,     # total_revenue
        "order_field": 5        # paying_users
    },
    "payment_rate_all": {
        "fetch_func": fetch_group_payment_rate_all_samples,
        "variation_field": 1,   # variation_id
        "date_field": 0,        # event_date
        "value_field": 4,       # purchase_rate
        "revenue_field": 3,     # paying_users
        "order_field": 2        # active_users
    },
    "payment_rate_new": {
        "fetch_func": fetch_group_payment_rate_new_samples,
        "variation_field": 1,   # variation_id
        "date_field": 0,        # event_date
        "value_field": 4,       # pay_rate_day1
        "revenue_field": 3,     # pay_user_day1
        "order_field": 2        # dnu
    },
    "click_rate": {
        "fetch_func": fetch_group_click_ratio_samples,
        "variation_field": 1,   # variation_id 在SQL返回的第几个字段（从0开始）
        "date_field": 0,        # event_date 在SQL返回的第几个字段
        "value_field": 4,       # click_ratio 在SQL返回的第几个字段
        "revenue_field": 3,     # clicked_events
        "order_field": 2        # showed_events
    },
    # 其它指标同理
}


if __name__ == "__main__":
    register_indicator_routes(app, INDICATOR_CONFIG)
    app.run(host="0.0.0.0", port=5050, debug=True)