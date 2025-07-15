import React from "react";
import { AbTestTrendChart } from "./Render/AbTestTrendChart";

/**
 * 渲染多个指标的趋势图（支持 all_trend 批量透传）
 * @param experimentName
 * @param startDate
 * @param endDate
 * @param metrics: 指标名数组（如 ["aov","arpu"]）
 * @param category: 指标分类
 * @param trendData: all_trend 接口返回的聚合数据对象
 */
export default function AbTestTrendChartList({
  experimentName,
  startDate,
  endDate,
  metrics = [],
  category,
  trendData = null // all_trend 批量数据
}) {
  const filteredMetrics = Array.isArray(metrics)
    ? metrics.filter(m => m && m !== "all")
    : [];

  if (!filteredMetrics.length) return null;

  return (
    <div>
      {filteredMetrics.map((metric) => (
        <div key={metric} style={{ margin: "36px 0" }}>
          <AbTestTrendChart
            experimentName={experimentName}
            startDate={startDate}
            endDate={endDate}
            metric={metric}
            category={category}
            trend={trendData ? trendData[metric] : null}  // ⭐ 用 trend 命名传递
          />
        </div>
      ))}
    </div>
  );
}
