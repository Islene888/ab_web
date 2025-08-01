// src/config/metricOptionsMap.js

export const metricOptionsMap = {
  business: [
    { value: 'aov', label: 'AOV' },
    { value: 'arpu', label: 'ARPU' },
    { value: 'arppu', label: 'ARPPU' },
    { value: 'payment_rate_all', label: 'Payment Rate All' },
    { value: 'payment_rate_new', label: 'Payment Rate New' },
    { value: 'aov_new_day', label: 'AOV New Day1' },
    { value: 'cancel_sub_day', label: 'Cancel Sub Day3' },
    { value: 'subscribe_new_day_aov', label: 'Subscribe New Day1 AOV' },
  ],
  chat: [
    { value: 'click_rate', label: 'Click Rate' },
    { value: 'explore_start_chat_rate', label: 'Explore Start Chat Rate' },
    { value: 'avg_chat_rounds', label: 'Avg Chat Rounds' },
    { value: 'first_chat_bot', label: 'First Chat Bot' },
    { value: 'avg_click_bots', label: 'Avg Click Bots' },
    { value: 'avg_time_spent', label: 'Avg Time Spent' },
    { value: 'explore_click_rate', label: 'Explore Click Rate' },
    { value: 'explore_avg_chat_rounds', label: 'Explore Avg Chat Rounds' },
  ],
  engagement: [
    { value: 'continue', label: 'Continue' },
    { value: 'conversation_reset', label: 'Conversation Reset' },
    { value: 'edit', label: 'Edit' },
    { value: 'follow', label: 'Follow' },
    { value: 'new_conversation', label: 'New Conversation' },
    { value: 'regen', label: 'Regen' },
  ],
  retention: [
    { value: 'all_retention_d1', label: 'Active User Day 1 Retention' },
    { value: 'all_retention_d3', label: 'Active User Day 3 Retention' },
    { value: 'all_retention_d7', label: 'Active User Day 7 Retention' },
    { value: 'all_retention_d15', label: 'Active User Day 15 Retention' },
    { value: 'new_retention_d1', label: 'New User Day 1 Retention' },
    { value: 'new_retention_d3', label: 'New User Day 3 Retention' },
    { value: 'new_retention_d7', label: 'New User Day 7 Retention' },
    { value: 'new_retention_d15', label: 'New User Day 15 Retention' },
  ],
  cohort: [
  { value: 'cumulative_retention_trend', label: 'Cul Retention Trend' },
  { value: 'cumulative_ltv_trend', label: 'Cul LTV Trend' },
  { value: 'cumulative_lt_trend', label: 'Cul LT Trend' },
  { value: 'all_retention_d1_heatmap', label: 'D1 Retention Heatmap' },
  { value: 'arpu_heatmap', label: 'ARPU Heatmap' },
  { value: 'time_spend_heatmap', label: 'Time Spent Heatmap' },
]};


