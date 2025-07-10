from backend.sql_jobs.Business.aov_sql import fetch_group_aov_samples
from backend.sql_jobs.Business.arpu_sql import fetch_group_arpu_samples
from backend.sql_jobs.Business.arppu_sql import fetch_group_arppu_samples
from backend.sql_jobs.Business.payment_rate_all_sql import fetch_group_payment_rate_all_samples
from backend.sql_jobs.Business.payment_rate_new_sql import fetch_group_payment_rate_new_samples
from backend.sql_jobs.chat_behavior.click_ratio_sql import fetch_group_click_ratio_samples
from backend.sql_jobs.chat_behavior.explore_chat_start_rate import fetch_group_explore_chat_start_rate_samples
from backend.sql_jobs.chat_behavior.avg_bot_click import fetch_group_avg_bot_click_samples
from backend.sql_jobs.chat_behavior.first_chat_bot import fetch_group_explore_chat_start_rate_samples as fetch_group_first_chat_bot_samples
from backend.sql_jobs.chat_behavior.show_click_rate import fetch_group_explore_chat_start_rate_samples as fetch_group_show_click_rate_samples
from backend.sql_jobs.chat_behavior.Chat_round import fetch_group_chat_round_samples
from backend.sql_jobs.chat_behavior.time_spend import fetch_group_time_spend_samples
from backend.sql_jobs.chat_behavior.explore_click_rate import fetch_group_explore_click_rate_samples
from backend.sql_jobs.chat_behavior.explore_chat_round import fetch_group_explore_chat_round_samples
from backend.sql_jobs.Engagement.Continue import fetch_group_continue_samples
from backend.sql_jobs.Engagement.Conversation_Reset import fetch_group_conversation_reset_samples
from backend.sql_jobs.Engagement.Edit import fetch_group_edit_samples
from backend.sql_jobs.Engagement.Follow import fetch_group_follow_samples
from backend.sql_jobs.Engagement.New_Conversation import fetch_group_new_conversation_samples
from backend.sql_jobs.Engagement.Regen import fetch_group_regen_samples
from backend.sql_jobs.Business.cancel_sub import fetch_group_cancel_sub_samples
from backend.sql_jobs.Business.subscribe_new import fetch_group_subscribe_new_samples
from backend.sql_jobs.Business.AOV_new import fetch_group_AOV_new_samples
from backend.sql_jobs.Business.payment_rate import fetch_group_payment_rate_samples
from backend.sql_jobs.Business.payment_rate import fetch_group_payment_rate_samples


INDICATOR_CONFIG = {
    # 1. 用户价值相关指标 8个
    "aov": {
        "fetch_func": fetch_group_aov_samples,
        "variation_field": 0,   # variation_id
        "date_field": 1,        # event_date
        "value_field": 4,       # aov 贝叶斯目标字段
        "revenue_field": 2,     # total_revenue 分子
        "order_field": 3        # total_order_cnt 分母
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
    "AOV_new_day": {
        "fetch_func": fetch_group_AOV_new_samples,  
        "variation_field": "variation_id",   # 建议都用字符串名
        "date_field": "event_date",
        "value_field": "aov_day1",  
        "revenue_field": "revenue_day1",
        "order_field": "order_cnt_day1"
    },
    "cancel_sub_day": {
        "fetch_func": fetch_group_cancel_sub_samples,
        "variation_field": "variation_id",      # 分组
        "date_field": "event_date",             # 日期
        "value_field": "unsub_rate_day3",       # 3天内退订率
        "revenue_field": "unsub_in3d",          # 3天内退订人数
        "order_field": "total_subs"             # 总订阅人数
    },
    "subscribe_new_day_aov": {
        "fetch_func": fetch_group_subscribe_new_samples,
        "variation_field": "variation_id",
        "date_field": "event_date",
        "value_field": "aov_subscribe_day1",            # 指向SQL里 day1 AOV 字段
        "revenue_field": "subscribe_revenue_day1",      # 指向 day1 收入
        "order_field": "subscribe_order_cnt_day1"       # 指向 day1 订单数
    },


    # 2. chat_behavior相关指标 8个
    "click_rate": {
        "fetch_func": fetch_group_click_ratio_samples,
        "variation_field": 1,   # 分组ID（数字下标，老SQL返回元组）
        "date_field": 0,        # 日期（数字下标，老SQL返回元组）
        "value_field": 4,       # 点击率（贝叶斯目标字段）
        "revenue_field": 3,     # 点击事件数（分子）
        "order_field": 2        # 展示事件数（分母）
    },
     "explore_start_chat_rate": {
        "fetch_func": fetch_group_explore_chat_start_rate_samples,
        "variation_field": "variation_id",   # 分组ID（别名）
        "date_field": "event_date",          # 日期（别名）
        "value_field": "chat_start_rate",    # 聊天启动率（贝叶斯目标字段）（别名）
        "revenue_field": "clicked_users",    # 点击用户数（分子）（别名）
        "order_field": "chat_users"          # 聊天用户数（分母）（别名）
    },
     "avg_chat_rounds": {
        "fetch_func": fetch_group_chat_round_samples,
        "variation_field": "variation",         # 分组ID
        "date_field": "event_date",            # 日期
        "value_field": "chat_depth_user",      # 平均每用户轮数（贝叶斯目标字段）
        "revenue_field": "total_chat_rounds",  # 总轮数（分子）
        "order_field": "unique_users",         # 总用户数（分母）
    },
     "first_chat_bot": { #人均开聊bot数
        "fetch_func": fetch_group_first_chat_bot_samples,
        "variation_field": 1,   # 分组ID（数字下标，老SQL返回元组）
        "date_field": 0,        # 日期（数字下标，老SQL返回元组）
        "value_field": 4,       # 平均每用户点击bot数（贝叶斯目标字段）
        "revenue_field": 2,     # 总点击数（分子）
        "order_field": 3        # 总用户数（分母）
    },
     "avg_click_bots": { #平均每用户点击bot数
        "fetch_func": fetch_group_avg_bot_click_samples,
        "variation_field": "variation_id",         # 分组ID
        "date_field": "event_date",                # 日期
        "value_field": "avg_bot_clicked",          # 平均每用户点击bot数（贝叶斯目标字段）
        "revenue_field": "total_click",            # 总点击数（分子）
        "order_field": "total_user",               # 总用户数（分母）
    },
    "avg_time_spent": {
        "fetch_func": fetch_group_time_spend_samples,
        "variation_field": "variation",     
        "date_field": "event_date",
        "value_field": "avg_time_spent_minutes",
        "revenue_field": "total_time_minutes",
        "order_field": "unique_users"
    },
    "explore_click_rate": {
        "fetch_func": fetch_group_explore_click_rate_samples,
        "variation_field": "variation",     
        "date_field": "event_date",
        "value_field": "click_rate",     # 点击率（贝叶斯目标字段）
        "revenue_field": "total_clicks", # 总点击数（分子）
        "order_field": "total_shows"     # 总展示数（分母） 
    },
    # 8.人均聊天轮次
    "explore_avg_chat_rounds": {
        "fetch_func": fetch_group_explore_chat_round_samples,
        "variation_field": "variation",     
        "date_field": "event_date",
        "value_field": "chat_depth_user",
        "revenue_field": "total_chat_rounds",
        "order_field": "unique_users"
    },

    # 3.engagement相关指标 6个
    "continue": {
        "fetch_func": fetch_group_continue_samples,
        "variation_field": "variation",     
        "date_field": "event_date",
        "value_field": "continue_ratio",
        "revenue_field": "total_continue",
        "order_field": "unique_continue_users"
    },
    "conversation_reset": {
        "fetch_func": fetch_group_conversation_reset_samples,
        "variation_field": "variation",     
        "date_field": "event_date",
        "value_field": "conversation_reset_ratio",
        "revenue_field": "total_conversation_reset",
        "order_field": "unique_conversation_reset_users"
    },
    "edit": {
        "fetch_func": fetch_group_edit_samples,
        "variation_field": "variation",     
        "date_field": "event_date",
        "value_field": "edit_ratio",
        "revenue_field": "total_edit",
        "order_field": "unique_edit_users"
    },
    "follow": {
        "fetch_func": fetch_group_follow_samples,
        "variation_field": "variation",     
        "date_field": "event_date",
        "value_field": "follow_ratio",
        "revenue_field": "total_follow",
        "order_field": "unique_follow_users"
    },
    "new_conversation": {
        "fetch_func": fetch_group_new_conversation_samples,
        "variation_field": "variation",     
        "date_field": "event_date",
        "value_field": "new_conversation_ratio",
        "revenue_field": "total_new_conversation",
        "order_field": "unique_new_conversation_users"
    },
    "regen": {
        "fetch_func": fetch_group_regen_samples,
        "variation_field": "variation",     
        "date_field": "event_date",
        "value_field": "regen_ratio",
        "revenue_field": "total_regen",
        "order_field": "unique_regen_users"
    }
    # 4. 其它指标同理，保持key与前端请求一致，字段用字符串名
}
