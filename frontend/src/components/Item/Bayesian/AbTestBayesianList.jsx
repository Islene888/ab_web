import React from "react";
import { Spin, Alert } from "antd";
import AbTestBayesianTable from "./Render/AbTestBayesianTable";

// 支持单指标和 all 模式
export function AbTestBayesianList({ loading, error, data, mode = "single", metric }) {
  if (loading) return <Spin style={{ display: 'block', margin: '40px auto', textAlign: 'center' }} />;
  if (error) return <Alert type="error" message={error} showIcon />;
  if (!data) return null;

  if (mode === "all" || mode === "all_in_one") {
    return (
      <div>
        {Object.entries(data).map(([metricName, metricData]) => {
          if (metricData.error) {
            return (
              <div key={metricName} style={{ margin: "48px auto", maxWidth: 1400, width: "98%" }}>
                <Alert type="error" message={`加载指标 ${metricName} 失败: ${metricData.error}`} showIcon />
              </div>
            );
          }
          return (
            <div key={metricName} style={{ margin: "6px auto", maxWidth: 1400, width: "98%" }}>
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

  return (
    <AbTestBayesianTable
      metric={metric}
      data={data.groups || []}
    />
  );
}
