import React from "react";
import { Spin, Alert } from "antd";
import AbTestBayesianTable from "./Render/AbTestBayesianTable";

// 支持单指标、all和all_in_one模式
export function AbTestBayesianList({ loading, error, data, mode = "single", metric }) {
  if (loading) return <Spin style={{ display: 'block', margin: '40px auto', textAlign: 'center' }} />;
  if (error) return <Alert type="error" message={error} showIcon />;
  if (!data) return null;

  // all模式/all_in_one模式支持渲染多个指标
  if (mode === "all" || mode === "all_in_one") {
    // 如果后端返回的数据是 null/undefined/[]，提前判空
    const entries = Object.entries(data || {}).filter(
      ([metricName, metricData]) => metricData && (Array.isArray(metricData.groups) ? metricData.groups.length > 0 : true)
    );
    if (entries.length === 0) {
      return (
        <div style={{ color: "#fff", textAlign: "center", padding: 32, minHeight: 100 }}>
          暂无有效的对比数据
        </div>
      );
    }

    return (
      <div>
        {entries.map(([metricName, metricData], idx) => {
          if (metricData && metricData.error) {
            return (
              <div key={metricName} style={{ margin: "42px auto", maxWidth: 1400, width: "98%" }}>
                <Alert type="error" message={`加载指标 ${metricName} 失败: ${metricData.error}`} showIcon />
              </div>
            );
          }
          return (
            <div
              key={metricName}
              style={{
                margin: idx === 0 ? "6px auto 36px" : "56px auto 36px", // 第一项间隔略小，后续大一点
                maxWidth: 1400,
                width: "98%",
              }}
            >
              <AbTestBayesianTable
                metric={metricName}
                data={metricData.groups || []}
              />
            </div>
          );
        })}
      </div>
    );
  }

  // 单指标模式
  return (
    <AbTestBayesianTable
      metric={metric}
      data={data.groups || []}
    />
  );
}

export default AbTestBayesianList;
