import React from "react";
import { AbTestTrendChart }  from "./Render/AbTestTrendChart";

/**
 * 渲染多个指标的趋势图
 * @param experimentName
 * @param startDate
 * @param endDate
 * @param metrics: 指标名数组（如 ["aov","arpu"]）
 */
export default function AbTestTrendChartList({
  experimentName,
  startDate,
  endDate,
  metrics = [],
}) {
  if (!metrics || metrics.length === 0) return null;

  return (
    <div>
      {metrics.map((metric, idx) => (
        <AbTestTrendChart
          key={metric}
          experimentName={experimentName}
          startDate={startDate}
          endDate={endDate}
          metric={metric}
        />
      ))}
    </div>
  );
}
