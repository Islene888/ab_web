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
        "variation_field": 0,
        "date_field": 1,
        "value_field": 4,
        "revenue_field": 2,
        "order_field": 3,
        "category": "business"
    },
    "arpu": {
        "fetch_func": fetch_group_arpu_samples,
        "variation_field": 0,
        "date_field": 1,
        "value_field": 4,
        "revenue_field": 2,
        "order_field": 3,
        "category": "business"
    },
    "arppu": {
        "fetch_func": fetch_group_arppu_samples,
        "variation_field": 0,
        "date_field": 1,
        "value_field": 7,
        "revenue_field": 4,
        "order_field": 5,
        "category": "business"
    },
    "payment_rate_all": {
        "fetch_func": fetch_group_payment_rate_all_samples,
        "variation_field": 1,
        "date_field": 0,
        "value_field": 4,
        "revenue_field": 3,
        "order_field": 2,
        "category": "business"
    },
    "payment_rate_new": {
        "fetch_func": fetch_group_payment_rate_new_samples,
        "variation_field": 1,
        "date_field": 0,
        "value_field": 4,
        "revenue_field": 3,
        "order_field": 2,
        "category": "business"
    },
    "AOV_new_day": {
        "fetch_func": fetch_group_AOV_new_samples,
        "variation_field": "variation_id",
        "date_field": "event_date",
        "value_field": "aov_day1",
        "revenue_field": "revenue_day1",
        "order_field": "order_cnt_day1",
        "category": "business"
    },
    "cancel_sub_day": {
        "fetch_func": fetch_group_cancel_sub_samples,
        "variation_field": "variation_id",
        "date_field": "event_date",
        "value_field": "unsub_rate_day3",
        "revenue_field": "unsub_in3d",
        "order_field": "total_subs",
        "category": "business"
    },
    "subscribe_new_day_aov": {
        "fetch_func": fetch_group_subscribe_new_samples,
        "variation_field": "variation_id",
        "date_field": "event_date",
        "value_field": "aov_subscribe_day1",
        "revenue_field": "subscribe_revenue_day1",
        "order_field": "subscribe_order_cnt_day1",
        "category": "business"
    },

    # 2. chat_behavior相关指标 8个
    "click_rate": {
        "fetch_func": fetch_group_click_ratio_samples,
        "variation_field": 1,
        "date_field": 0,
        "value_field": 4,
        "revenue_field": 3,
        "order_field": 2,
        "category": "chat"
    },
    "explore_start_chat_rate": {
        "fetch_func": fetch_group_explore_chat_start_rate_samples,
        "variation_field": "variation_id",
        "date_field": "event_date",
        "value_field": "chat_start_rate",
        "revenue_field": "clicked_users",
        "order_field": "chat_users",
        "category": "chat"
    },
    "avg_chat_rounds": {
        "fetch_func": fetch_group_chat_round_samples,
        "variation_field": "variation",
        "date_field": "event_date",
        "value_field": "chat_depth_user",
        "revenue_field": "total_chat_rounds",
        "order_field": "unique_users",
        "category": "chat"
    },
    "first_chat_bot": {
        "fetch_func": fetch_group_first_chat_bot_samples,
        "variation_field": 1,
        "date_field": 0,
        "value_field": 4,
        "revenue_field": 2,
        "order_field": 3,
        "category": "chat"
    },
    "avg_click_bots": {
        "fetch_func": fetch_group_avg_bot_click_samples,
        "variation_field": "variation_id",
        "date_field": "event_date",
        "value_field": "avg_bot_clicked",
        "revenue_field": "total_click",
        "order_field": "total_user",
        "category": "chat"
    },
    "avg_time_spent": {
        "fetch_func": fetch_group_time_spend_samples,
        "variation_field": "variation",
        "date_field": "event_date",
        "value_field": "avg_time_spent_minutes",
        "revenue_field": "total_time_minutes",
        "order_field": "unique_users",
        "category": "chat"
    },
    "explore_click_rate": {
        "fetch_func": fetch_group_explore_click_rate_samples,
        "variation_field": "variation",
        "date_field": "event_date",
        "value_field": "click_rate",
        "revenue_field": "total_clicks",
        "order_field": "total_shows",
        "category": "chat"
    },
    "explore_avg_chat_rounds": {
        "fetch_func": fetch_group_explore_chat_round_samples,
        "variation_field": "variation",
        "date_field": "event_date",
        "value_field": "chat_depth_user",
        "revenue_field": "total_chat_rounds",
        "order_field": "unique_users",
        "category": "chat"
    },

    # 3.engagement相关指标 6个
    "continue": {
        "fetch_func": fetch_group_continue_samples,
        "variation_field": "variation",
        "date_field": "event_date",
        "value_field": "continue_ratio",
        "revenue_field": "total_continue",
        "order_field": "unique_continue_users",
        "category": "engagement"
    },
    "conversation_reset": {
        "fetch_func": fetch_group_conversation_reset_samples,
        "variation_field": "variation",
        "date_field": "event_date",
        "value_field": "conversation_reset_ratio",
        "revenue_field": "total_conversation_reset",
        "order_field": "unique_conversation_reset_users",
        "category": "engagement"
    },
    "edit": {
        "fetch_func": fetch_group_edit_samples,
        "variation_field": "variation",
        "date_field": "event_date",
        "value_field": "edit_ratio",
        "revenue_field": "total_edit",
        "order_field": "unique_edit_users",
        "category": "engagement"
    },
    "follow": {
        "fetch_func": fetch_group_follow_samples,
        "variation_field": "variation",
        "date_field": "event_date",
        "value_field": "follow_ratio",
        "revenue_field": "total_follow",
        "order_field": "unique_follow_users",
        "category": "engagement"
    },
    "new_conversation": {
        "fetch_func": fetch_group_new_conversation_samples,
        "variation_field": "variation",
        "date_field": "event_date",
        "value_field": "new_conversation_ratio",
        "revenue_field": "total_new_conversation",
        "order_field": "unique_new_conversation_users",
        "category": "engagement"
    },
    "regen": {
        "fetch_func": fetch_group_regen_samples,
        "variation_field": "variation",
        "date_field": "event_date",
        "value_field": "regen_ratio",
        "revenue_field": "total_regen",
        "order_field": "unique_regen_users",
        "category": "engagement"
    }
    # 4. 其它指标同理，保持key与前端请求一致，字段用字符串名
}
