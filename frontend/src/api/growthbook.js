// src/api/growthbook.js

/**
 * 各 category 对应的 metric 列表
 */
export const metricOptionsMap = {
  business: [
    { value: 'aov', label: 'AOV' },
    { value: 'arpu', label: 'ARPU' },
    { value: 'arppu', label: 'ARPPU' },
    { value: 'payment_rate_all', label: 'Payment Rate All' },
    { value: 'payment_rate_new', label: 'Payment Rate New Users' },
    { value: 'AOV_new_day', label: 'AOV New Users' },
    { value: 'cancel_sub_day', label: 'Cancel Rate 3 Days' },
    { value: 'subscribe_new_day_aov', label: 'Subscribe AOV New Users Day1' },
  ],
  engagement: [
    { value: 'continue', label: 'Continue' },
    { value: 'conversation_reset', label: 'Conversation Reset' },
    { value: 'edit', label: 'Edit' },
    { value: 'follow', label: 'Follow' },
    { value: 'message', label: 'Message' },
    { value: 'new_conversation', label: 'New Conversation' },
    { value: 'regen', label: 'Regen' },
  ],
  retention: [
    { value: 'all_retention', label: 'All Retention' },
    { value: 'new_retention', label: 'New Retention' },
  ],
  recharge: [
    { value: 'recharge_rate', label: 'Recharge Rate' },
  ],
  chat: [
    { value: 'click_rate', label: 'Click Rate' },
    { value: 'explore_start_chat_rate', label: 'Explore Start Chat Rate' },
    { value: 'avg_chat_rounds', label: 'Avg Chat Rounds per User' },
    { value: 'avg_start_chat_bots', label: 'Avg Start Chat Bots per User' },
    { value: 'avg_click_bots', label: 'Avg Click Bots per User' },
    { value: 'avg_time_spent', label: 'Avg Time Spent per User' },
    { value: 'explore_click_rate', label: 'Explore Click Rate' },
    { value: 'explore_avg_chat_rounds', label: 'Explore Avg Chat Rounds per User' },
  ],
};

/** 获取实验列表 */
export async function fetchExperiments() {
  const res = await fetch('/api/experiments');
  if (!res.ok) {
    console.error('Failed to fetch experiments', res.status, res.statusText);
    return [];
  }
  return res.json();
}

/** 获取单个 metric 的贝叶斯结果 */
export async function fetchMetricBayesian(metric, experimentName, startDate, endDate) {
  const url = `/api/${metric}_bayesian`
    + `?experiment_name=${encodeURIComponent(experimentName)}`
    + `&start_date=${startDate}&end_date=${endDate}`;
  const res = await fetch(url);
  if (!res.ok) return { groups: [] };
  return res.json();
}

export async function fetchTrendData(metric, experimentName, startDate, endDate) {
  const url = `/api/${metric}_trend`
    + `?experiment_name=${encodeURIComponent(experimentName)}`
    + `&start_date=${startDate}&end_date=${endDate}`;
  const res = await fetch(url);
  if (!res.ok) return { data: [] };
  return res.json();
}

