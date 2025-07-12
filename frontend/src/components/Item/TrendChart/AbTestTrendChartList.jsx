import React from "react";
import { AbTestTrendChart } from "./Render/AbTestTrendChart";

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
        <div
          key={metric}
          style={{
            margin: "36px 0",  // 控制间隔为12px，避免太大的间隔
            background: "none",
          }}
        >
          {/* 如果你不需要标题，删除或注释掉这个部分 */}
          {/* <div
            style={{
              color: "#FFD700",
              fontWeight: 900,
              fontSize: 22,
              letterSpacing: 1,
              marginBottom: 10,
              marginLeft: 4,
              fontFamily: "Inter, Roboto, PingFang SC, sans-serif",
              textShadow: "0 2px 12px #3B6FF544",
            }}
          >
            {metric && metric.toUpperCase()}
          </div> */}

          <AbTestTrendChart
            experimentName={experimentName}
            startDate={startDate}
            endDate={endDate}
            metric={metric}
          />
        </div>
      ))}
    </div>
  );
}
