import React, { useState, useEffect } from 'react';
import ReactECharts from 'echarts-for-react';

// 趋势图组件（完善极端情况处理）
export function AbTestTrendChart({ experimentName, startDate, endDate, metric, userType }) {
  const [trend, setTrend] = useState(null);

  useEffect(() => {
    if (!experimentName || !startDate || !endDate || !metric) return;
    const userTypeParam = userType ? `&user_type=${userType}` : '';
    fetch(`/api/${metric}_trend?experiment_name=${experimentName}&start_date=${startDate}&end_date=${endDate}${userTypeParam}`)
      .then(res => res.json())
      .then(setTrend)
      .catch(() => setTrend(null));
  }, [experimentName, startDate, endDate, metric, userType]);

  // 1. 数据不存在时不渲染
  if (!trend || !trend.dates || !trend.series) return null;

  // 2. 只渲染有「任意大于0」数据的series（全0/null/负不渲染）
  const filteredSeries = trend.series.filter(s =>
    Array.isArray(s.data) && s.data.some(v => typeof v === 'number' && v > 0)
  );
  const filteredDates = trend.dates;

  // 3. 计算所有线的数据的最小/最大
  const allData = filteredSeries.flatMap(s => s.data).filter(v => typeof v === 'number' && !isNaN(v));
  let minData = Math.min(...allData);
  let maxData = Math.max(...allData);

  // 4. 全部一样/只有一个点：上下浮动10%
  if (allData.length === 0 || isNaN(minData) || isNaN(maxData)) {
    // 数据全是null/NaN
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
  // 5. 防止很接近0时画不出来
  if (Math.abs(maxData) < 1e-6 && Math.abs(minData) < 1e-6) {
    return <div style={{color:'#fff',textAlign:'center',padding:32}}>暂无有效趋势数据</div>;
  }

  // 6. 配色风格
  const bg = "#23243a";
  const cardShadow = "0 6px 32px 0 rgba(0,0,0,0.13)";
  const border = "#2d2f4a";
  const mainFont = "#fff";
  const subFont = "#7c819a";
  const thColor = "#dbe2f9";

  // 7. 渲染
  return (
    <div style={{
      background: bg,
      borderRadius: 0,
      boxShadow: cardShadow,
      maxWidth: "100%",
      margin: "0 auto 0px auto",  // Adjust the bottom margin here
      padding: 0
    }}>
      <div style={{ width: "100%", padding: 32 }}>
        <ReactECharts
          option={{
            backgroundColor: bg,
            tooltip: {
              trigger: 'axis',
              backgroundColor: "#23243a",
              borderColor: border,
              borderWidth: 1.5,
              textStyle: { color: mainFont, fontWeight: 700, fontSize: 16 },
              formatter: (params) => {
                let html = `<span style='color:${mainFont};font-weight:700'>${params[0]?.axisValueLabel}</span><br/>`;
                params.forEach(item => {
                  const group = filteredSeries[item.seriesIndex];
                  const revenueArr = group.revenue || [];
                  const orderArr = group.order || [];
                  const idx = item.dataIndex;
                  const revenue = revenueArr && revenueArr[idx] !== undefined ? Math.round(revenueArr[idx]) : '-';
                  const order = orderArr && orderArr[idx] !== undefined ? Math.round(orderArr[idx]) : '-';
                  html += `<span style=\"display:inline-block;margin-right:8px;border-radius:10px;width:10px;height:10px;background:${item.color}\"></span>`;
                  html += `${item.seriesName}: <b style='color:${mainFont}'>${item.data}</b> <span style='color:${subFont}'>(${revenue} / ${order})</span><br/>`;
                });
                return html;
              }
            },
            legend: {
              show: false,  // 隐藏右上角的 legend
              data: filteredSeries.map(s => s.variation),
              textStyle: { color:"#FFD700", fontWeight: 700, fontSize: 16 },  // 改为金色
              top: 0,
              right: 0,
              orient: 'vertical'
            },
            grid: { left: 60, right: 100, top: 60, bottom: 40, borderColor: border },
            xAxis: {
              type: 'category',
              data: filteredDates.map(d => d.slice(0, 10)),
              boundaryGap: false,
              axisLine: { lineStyle: { color: border, width: 2 } },
              axisLabel: { color: thColor, fontWeight: 700, fontSize: 15 },
              splitLine: { show: false }
            },
            yAxis: {
              type: 'value',
              name: metric,
              nameTextStyle: {
                color: "#FFB300",
                fontWeight: 700,
                fontSize: 20,
                padding: [20, 0, 0, -40],
                align: 'left',
              },
              axisLine: { lineStyle: { color: border, width: 2 } },
              axisLabel: { color: thColor, fontWeight: 700, fontSize: 15 },
              splitLine: { show: false, lineStyle: { color: border, width: 1, type: 'dashed', opacity: 0.3 } },
              min: minData,
              max: maxData
            },
            series: filteredSeries.map((s, idx) => ({
              name: s.variation,
              type: 'line',
              data: s.data,
              smooth: true,
              symbol: 'circle',
              showSymbol: false,
              lineStyle: { width: 3, color: idx === 0 ? '#3B6FF5' : '#FF9900' },
              itemStyle: { color: idx === 0 ? '#3B6FF5' : '#FF9900' },
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
