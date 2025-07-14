import React, { useState, useEffect } from 'react';
import ReactECharts from 'echarts-for-react';

export function AbTestTrendChart({ experimentName, startDate, endDate, metric, category, userType }) {
  const [trend, setTrend] = useState(null);
  const [loading, setLoading] = useState(false);

  // 配色表，多实验组自动分配不同颜色
  const colorList = [
    "#3B6FF5", "#FF9900", "#20C997", "#E34F4F", "#6F42C1",
    "#FFA500", "#0099CC", "#FF66CC", "#FFC300", "#4A90E2"
  ];

  useEffect(() => {
    if (!experimentName || !startDate || !endDate || !metric) {
      setTrend(null);
      return;
    }
    setLoading(true);
    setTrend(null); // 清空残影
    const userTypeParam = userType ? `&user_type=${userType}` : '';
    const categoryParam = category ? `&category=${category}` : '';
    fetch(`/api/${metric}_trend?experiment_name=${experimentName}&start_date=${startDate}&end_date=${endDate}${userTypeParam}&metric=${metric}${categoryParam}`)
      .then(res => res.json())
      .then(res => setTrend(res))
      .catch(() => setTrend(null))
      .finally(() => setLoading(false));
  }, [experimentName, startDate, endDate, metric, category, userType]);

  if (loading) {
    return (
      <div style={{ height: 260, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <span style={{ color: '#fff' }}>加载中...</span>
      </div>
    );
  }

  if (!trend || !trend.dates || !trend.series) {
    return (
      <div style={{color:'#fff',textAlign:'center',padding:32}}>暂无有效趋势数据</div>
    );
  }

  const filteredSeries = trend.series.filter(s =>
    Array.isArray(s.data) && s.data.some(v => typeof v === 'number' && v > 0)
  );
  const filteredDates = trend.dates;

  const allData = filteredSeries.flatMap(s => s.data).filter(v => typeof v === 'number' && !isNaN(v));
  let minData = Math.min(...allData);
  let maxData = Math.max(...allData);

  if (allData.length === 0 || isNaN(minData) || isNaN(maxData)) {
    return <div style={{color:'#fff',textAlign:'center',padding:32}}>暂无有效趋势数据</div>;
  }
  if (minData === maxData) {
    minData = minData * 0.95;
    maxData = maxData * 1.05;
  } else {
    const padding = (maxData - minData) * 0.1;
    minData = minData - padding;
    maxData = maxData + padding;
  }
  if (Math.abs(maxData) < 1e-6 && Math.abs(minData) < 1e-6) {
    return <div style={{color:'#fff',textAlign:'center',padding:32}}>暂无有效趋势数据</div>;
  }

  // 颜色和legend自动匹配
  const legendData = filteredSeries.map(s => s.variation || s.name || "Group");
  return (
    <div style={{
      background: "#23243a",
      borderRadius: 0,
      boxShadow: "0 6px 32px 0 rgba(0,0,0,0.13)",
      maxWidth: "100%",
      margin: "0 auto 0px auto",
      padding: 0
    }}>
      <div style={{ width: "100%", padding: 32 }}>
        <ReactECharts
          option={{
            backgroundColor: "#23243a",
            color: colorList,
            tooltip: {
              trigger: 'axis',
              backgroundColor: "#23243a",
              borderColor: "#2d2f4a",
              borderWidth: 1.5,
              textStyle: { color: "#fff", fontWeight: 700, fontSize: 16 },
              formatter: (params) => {
                let html = `<span style='color:#fff;font-weight:700'>${params[0]?.axisValueLabel}</span><br/>`;
                params.forEach(item => {
                  const group = filteredSeries[item.seriesIndex];
                  const revenueArr = group.revenue || [];
                  const orderArr = group.order || [];
                  const idx = item.dataIndex;
                  const revenue = revenueArr && revenueArr[idx] !== undefined ? Math.round(revenueArr[idx]) : '-';
                  const order = orderArr && orderArr[idx] !== undefined ? Math.round(orderArr[idx]) : '-';
                  html += `<span style="display:inline-block;margin-right:8px;border-radius:10px;width:10px;height:10px;background:${item.color}"></span>`;
                  html += `${item.seriesName}: <b style='color:#fff'>${item.data}</b> <span style='color:#7c819a'>(${revenue} / ${order})</span><br/>`;
                });
                return html;
              }
            },
            legend: {
              show: true,
              data: legendData,
              orient: 'vertical',
              right: 16,
              top: 32,
              itemWidth: 18,
              itemHeight: 12,
              icon: 'rect',
              textStyle: {
                color: "#FFD700",
                fontWeight: 700,
                fontSize: 16
              }
            },
            grid: { left: 60, right: 120, top: 60, bottom: 40, borderColor: "#2d2f4a" },
            xAxis: {
              type: 'category',
              data: filteredDates.map(d => d.slice(0, 10)),
              boundaryGap: false,
              axisLine: { lineStyle: { color: "#2d2f4a", width: 2 } },
              axisLabel: { color: "#dbe2f9", fontWeight: 700, fontSize: 15 },
              splitLine: { show: false }
            },
            yAxis: {
              type: 'value',
              name: metric ? metric.toUpperCase() : '',
              nameTextStyle: {
                color: "#FFB300",
                fontWeight: 700,
                fontSize: 15,
                padding: [0, 10, 20, -60],
                align: 'left',
              },
              axisLine: { lineStyle: { color: "#2d2f4a", width: 2 } },
              axisLabel: { color: "#dbe2f9", fontWeight: 700, fontSize: 15 },
              splitLine: { show: false, lineStyle: { color: "#2d2f4a", width: 1, type: 'dashed', opacity: 0.3 } },
              min: minData,
              max: maxData
            },
            series: filteredSeries.map((s, idx) => ({
              name: legendData[idx],
              type: 'line',
              data: s.data,
              smooth: true,
              symbol: 'circle',
              showSymbol: false,
              lineStyle: { width: 3, color: colorList[idx % colorList.length] },
              itemStyle: { color: colorList[idx % colorList.length] },
            }))
          }}
          style={{ height: 260, width: "100%" }}
          notMerge
          lazyUpdate
        />
      </div>
    </div>
  );
}
