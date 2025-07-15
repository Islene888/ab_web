// src/api/AbtestApi.js


export async function fetchBayesian({ experimentName, startDate, endDate, metric, category }) {
  const url = `/api/${metric}_bayesian?experiment_name=${encodeURIComponent(experimentName)}&start_date=${startDate}&end_date=${endDate}&metric=${metric}&category=${category}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error("API error");
  return res.json();
}


export async function fetchAllBayesian({ experimentName, startDate, endDate, category }) {
  const url = `/api/all_bayesian?experiment_name=${encodeURIComponent(experimentName)}&start_date=${startDate}&end_date=${endDate}&category=${category}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error("API error");
  return res.json();
}


export async function fetchAllInOneBayesian({ experimentName, startDate, endDate }) { // 移除 category 参数
  const url = `/api/all_category_all_metrics?experiment_name=${encodeURIComponent(experimentName)}&start_date=${startDate}&end_date=${endDate}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error("API error");
  return res.json();
}

