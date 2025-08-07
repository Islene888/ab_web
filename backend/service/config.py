from ..sql_jobs.Business.aov_sql import fetch_group_aov_samples
from ..sql_jobs.Business.arpu_sql import fetch_group_arpu_samples
from ..sql_jobs.Business.arppu_sql import fetch_group_arppu_samples
from ..sql_jobs.Business.cohort_arpu import fetch_cohort_arpu_heatmap
from ..sql_jobs.Business.payment_rate_all_sql import fetch_group_payment_rate_all_samples
from ..sql_jobs.Business.payment_rate_new_sql import fetch_group_payment_rate_new_samples
from ..sql_jobs.Retention.cohort_retention import fetch_cohort_retention_heatmap
from ..sql_jobs.chat_behavior.click_ratio_sql import fetch_group_click_ratio_samples
from ..sql_jobs.chat_behavior.cohort_time_spend import fetch_cohort_time_spent_heatmap
from ..sql_jobs.chat_behavior.explore_chat_start_rate import fetch_group_explore_chat_start_rate_samples
from ..sql_jobs.chat_behavior.avg_bot_click import fetch_group_avg_bot_click_samples
from ..sql_jobs.chat_behavior.first_chat_bot import fetch_group_explore_chat_start_rate_samples as fetch_group_first_chat_bot_samples
from ..sql_jobs.chat_behavior.Chat_round import fetch_group_chat_round_samples
from ..sql_jobs.chat_behavior.time_spend import fetch_group_time_spend_samples
from ..sql_jobs.chat_behavior.explore_click_rate import fetch_group_explore_click_rate_samples
from ..sql_jobs.chat_behavior.explore_chat_round import fetch_group_explore_chat_round_samples
from ..sql_jobs.Engagement.Continue import fetch_group_continue_samples
from ..sql_jobs.Engagement.Conversation_Reset import fetch_group_conversation_reset_samples
from ..sql_jobs.Engagement.Edit import fetch_group_edit_samples
from ..sql_jobs.Engagement.Follow import fetch_group_follow_samples
from ..sql_jobs.Engagement.New_Conversation import fetch_group_new_conversation_samples
from ..sql_jobs.Engagement.Regen import fetch_group_regen_samples
from ..sql_jobs.Business.cancel_sub import fetch_group_cancel_sub_samples
from ..sql_jobs.Business.subscribe_new import fetch_group_subscribe_new_samples
from ..sql_jobs.Business.AOV_new import fetch_group_AOV_new_samples
from ..sql_jobs.Retention.active_retention import fetch_active_user_retention
from ..sql_jobs.Retention.new_retention import fetch_new_user_retention
from ..sql_jobs.Retention.cul_retention import fetch_group_cumulative_retained_users_daily
from ..sql_jobs.Business.cul_ltv import fetch_group_cumulative_ltv_daily
from ..sql_jobs.chat_behavior.cul_lt import fetch_group_cumulative_lt_daily

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
    "aov_new_day": {
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
    },
    # 4. 留存指标 8个（
    "all_retention_d1": {
        "fetch_func": lambda experiment_name, start_date, end_date, engine: fetch_active_user_retention(experiment_name, start_date, end_date, engine, day=1),
        "variation_field": "variation",         # 根据你的SQL返回的字段名设置
        "date_field": "active_date",
        "value_field": "d1_retention_rate",     # 字段名要和fetch返回dict一致
        "revenue_field": "d1_retained_users",   # 这里用实际人数字段名
        "order_field": "active_users",
        "category": "retention"
    },
    "all_retention_d3": {
        "fetch_func": lambda experiment_name, start_date, end_date, engine: fetch_active_user_retention(experiment_name, start_date, end_date, engine, day=3),
        "variation_field": "variation",
        "date_field": "active_date",
        "value_field": "d3_retention_rate",
        "revenue_field": "d3_retained_users",
        "order_field": "active_users",
        "category": "retention"
    },
    "all_retention_d7": {
        "fetch_func": lambda experiment_name, start_date, end_date, engine: fetch_active_user_retention(experiment_name, start_date, end_date, engine, day=7),
        "variation_field": "variation",
        "date_field": "active_date",
        "value_field": "d7_retention_rate",
        "revenue_field": "d7_retained_users",
        "order_field": "active_users",
        "category": "retention"
    },
    "all_retention_d15": {
        "fetch_func": lambda experiment_name, start_date, end_date, engine: fetch_active_user_retention(experiment_name, start_date, end_date, engine, day=15),
        "variation_field": "variation",
        "date_field": "active_date",
        "value_field": "d15_retention_rate",
        "revenue_field": "d15_retained_users",
        "order_field": "active_users",
        "category": "retention"
    },
    "new_retention_d1": {
        "fetch_func": lambda experiment_name, start_date, end_date, engine: fetch_new_user_retention(experiment_name, start_date, end_date, engine, day=1),
        "variation_field": "variation",
        "date_field": "first_visit_date",
        "value_field": "d1_retention_rate",
        "revenue_field": "d1_retained_users",
        "order_field": "new_users",
        "category": "retention"
    },
    "new_retention_d3": {
        "fetch_func": lambda experiment_name, start_date, end_date, engine: fetch_new_user_retention(experiment_name, start_date, end_date, engine, day=3),
        "variation_field": "variation",
        "date_field": "first_visit_date",
        "value_field": "d3_retention_rate",
        "revenue_field": "d3_retained_users",
        "order_field": "new_users",
        "category": "retention"
    },
    "new_retention_d7": {
        "fetch_func": lambda experiment_name, start_date, end_date, engine: fetch_new_user_retention(experiment_name, start_date, end_date, engine, day=7),
        "variation_field": "variation",
        "date_field": "first_visit_date",
        "value_field": "d7_retention_rate",
        "revenue_field": "d7_retained_users",
        "order_field": "new_users",
        "category": "retention"
    },
    "new_retention_d15": {
        "fetch_func": lambda experiment_name, start_date, end_date, engine: fetch_new_user_retention(experiment_name, start_date, end_date, engine, day=15),
        "variation_field": "variation",
        "date_field": "first_visit_date",
        "value_field": "d15_retention_rate",
        "revenue_field": "d15_retained_users",
        "order_field": "new_users",
        "category": "retention"
    },
    # --- Cumulative 累计指标 ---
    "cumulative_retention": {
        "fetch_func": fetch_group_cumulative_retained_users_daily,
        "variation_field": "variation_id",  # SQL 返回字段名
        "date_field": "event_date",
        "value_field": "cumulative_retention_rate",  # 返回的留存率
        "revenue_field": "cumulative_retained_users",  # 分子
        "order_field": "cumulative_registered_users",  # 分母
        "category": "retention"
    },
    "cumulative_ltv": {
        "fetch_func": fetch_group_cumulative_ltv_daily,
        "variation_field": "variation_id",
        "date_field": "event_date",
        "value_field": "cumulative_ltv",  # 累计LTV
        "revenue_field": "cumulative_revenue",  # 总收入
        "order_field": "cumulative_users",  # 总活跃人数
        "category": "business"
    },
    "cumulative_lt": {
        "fetch_func": fetch_group_cumulative_lt_daily,
        "variation_field": "variation_id",
        "date_field": "event_date",
        "value_field": "cumulative_lt",  # 累计LT
        "revenue_field": "cumulative_time_minutes",  # 总分钟数
        "order_field": "cumulative_users",  # 总活跃人数
        "category": "engagement"
    },
    # "cohort_arpu": {
    #     "fetch_func": fetch_cohort_arpu_heatmap,
    #     "variation_field": "variation_id",
    #     "date_field": "register_date",
    #     "value_field": "arpu",
    #     "revenue_field": "total_revenue",
    #     "order_field": "active_users",
    #     "category": "business"
    # },
    "cohort_arpu": {
        "fetch_func": fetch_cohort_arpu_heatmap,
        "variation_field": "variation_id",
        "date_field": "register_date",
        "extra_field": "cohort_day",
        "value_field": "ltv",
        "revenue_field": "total_revenue",
        "order_field": "active_users",
        "category": "business",
        "result_type": "heatmap"
    },
    "cohort_retention_heatmap": {
        "fetch_func": fetch_cohort_retention_heatmap,
        "variation_field": "variation_id",
        "date_field": "register_date",
        "extra_field": "cohort_day",
        "value_field": "retention_rate",
        "revenue_field": "retained_users",
        "order_field": "new_users",
        "category": "retention",
        "result_type": "heatmap"
    },
    "cohort_time_spent_heatmap": {
        "fetch_func": fetch_cohort_time_spent_heatmap,
        "variation_field": "variation_id",
        "date_field": "register_date",
        "extra_field": "cohort_day",
        "value_field": "avg_time_spent_minutes",
        "revenue_field": "total_time_spent",
        "order_field": "active_users",
        "category": "chat",
        "result_type": "heatmap"
    }



}
