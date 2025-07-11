// AbTestTrendChart.jsx

import React, { useState, useEffect } from 'react';
import ReactECharts from 'echarts-for-react';

/**
 * 趋势图组件
 * @param {string} experimentName 实验名
 * @param {string} startDate 开始日期
 * @param {string} endDate 结束日期
 * @param {string|array} metric 指标名
 */
export default function AbTestTrendChart({ experimentName, startDate, endDate, metric }) {
  const [trend, setTrend] = useState(null);

  useEffect(() => {
    const metricName = Array.isArray(metric) ? metric[0] : metric;
    if (!experimentName || !startDate || !endDate || !metricName) return;

    // 拉取趋势数据
    fetch(`/api/${metricName}_trend?experiment_name=${experimentName}&start_date=${startDate}&end_date=${endDate}`)
      .then(res => res.json())
      .then(setTrend)
      .catch(() => setTrend(null));
  }, [experimentName, startDate, endDate, metric]);

  if (!trend || !trend.dates || !trend.series || trend.series.length === 0) {
    return null; // 没有趋势数据就不渲染
  }

  // 视觉风格参数，可根据需要调整
  const bg = "#23243a";
  // const border = "#2d2f4a";
  // const mainFont = "#fff";
  // const thColor = "#dbe2f9";

  return (
    <div style={{
      background: bg,
      borderRadius: 0,
      boxShadow: "0 6px 32px 0 rgba(0,0,0,0.13)",
      maxWidth: "100%",
      margin: "0 auto 48px auto",
      padding: 0
    }}>
      <div style={{ width: "100%", padding: 32 }}>
        <ReactECharts
          option={{
            backgroundColor: bg,
            tooltip: {
              trigger: 'axis',
              axisPointer: { type: 'cross' }
            },
            legend: { data: trend.series.map(s => s.variation), textStyle: { color: '#cfd6f4' } },
            grid: { left: 40, right: 20, bottom: 30, top: 50, containLabel: true },
            xAxis: {
              type: 'category',
              data: trend.dates.map(d => d.slice(0, 10)),
              axisLine: { lineStyle: { color: '#53567d' } },
              axisLabel: { color: '#cfd6f4', fontWeight: 700, fontSize: 15 },
            },
            yAxis: {
              type: 'value',
              name: Array.isArray(metric) ? metric[0] : metric,
              nameTextStyle: { color: '#fff', fontSize: 14 },
              axisLine: { lineStyle: { color: '#53567d' } },
              axisLabel: { color: '#fff', fontWeight: 700, fontSize: 15 },
              splitLine: { lineStyle: { color: '#323353', type: 'dashed' } },
            },
            series: trend.series.map((s) => ({
              name: s.variation,
              type: 'line',
              data: s.data,
              smooth: true,
              showSymbol: true,
              lineStyle: { width: 4 },
              symbol: 'circle',
              symbolSize: 10
            }))
          }}
          style={{ height: 220, width: "100%" }}
          notMerge
          lazyUpdate
        />
      </div>
    </div>
  );
}
