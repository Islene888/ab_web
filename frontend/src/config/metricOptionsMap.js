// src/config/metricOptionsMap.js

export const metricOptionsMap = {
  business: [
    { value: 'aov', label: 'AOV' },
    { value: 'arpu', label: 'ARPU' },
    { value: 'arppu', label: 'ARPPU' },
    { value: 'payment_rate_all', label: 'Payment Rate All' },
    { value: 'payment_rate_new', label: 'Payment Rate New' },
    { value: 'AOV_new_day', label: 'AOV New Day1' },
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
    { value: 'all_retention', label: 'All Retention' },
    { value: 'new_retention', label: 'New Retention' },
  ]
};
