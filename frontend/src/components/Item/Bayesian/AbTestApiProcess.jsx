//AbTestApiProcess.jsx

import React, { useState, useEffect } from 'react';
import { Spin, Alert } from 'antd';
import { fetchBayesian, fetchAllBayesian, fetchAllInOneBayesian } from '../../../api/abtestApi';
import AbTestBayesianTable from './Render/AbTestBayesianTable';
import AbTestTrendChart from '../TrendChart/AbTestTrendChart';

// 容器组件，负责获取表格数据
export function AbTestApiProcess(props) {
  const {
    experimentName = "",
    startDate = "",
    endDate = "",
    metric = "",
    category = "",
    mode = "single",
  } = props;

  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // 数据获取逻辑
  useEffect(() => {
    if (!experimentName || !startDate || !endDate) return;
    if (mode === 'single' && !metric) return;
    if (mode === 'all' && !category) return;

    setLoading(true);
    setError(null);

    let fetchPromise;
    if (mode === "all") {
      fetchPromise = fetchAllBayesian({ experimentName, startDate, endDate, category });
    } else if (mode === "all_in_one") {
      fetchPromise = fetchAllInOneBayesian({ experimentName, startDate, endDate });
    } else {
      fetchPromise = fetchBayesian({ experimentName, startDate, endDate, metric });
    }

    fetchPromise
      .then(setData)
      .catch((err) => setError(err.message || "Error fetching data"))
      .finally(() => setLoading(false));
  }, [experimentName, startDate, endDate, metric, category, mode]);

  // --- 渲染逻辑 ---
  if (loading) return <Spin style={{ display: 'block', margin: '40px auto', textAlign: 'center' }} />;
  if (error) return <Alert type="error" message={error} showIcon />;
  if (!data) return null;

  // 根据 mode 决定如何渲染
  if (mode === "all" || mode === "all_in_one") {
    return (
      <div>
        {Object.entries(data).map(([metricName, metricData]) => {
          if (metricData.error) {
            return (
              <div key={metricName} style={{ margin: "48px auto", maxWidth: 1400, width: "98%" }}>
                <div style={{ color: "#bfc2d4", fontWeight: 900, fontSize: 28, margin: "0 0 16px 32px" }}>
                  {metricName.replace(/_/g, " ").toUpperCase()}
                </div>
                <Alert type="error" message={`加载指标 ${metricName} 失败: ${metricData.error}`} showIcon />
              </div>
            )
          }
          return (
            <div key={metricName} style={{ margin: "48px auto", maxWidth: 1400, width: "98%" }}>
              <div style={{ color: "#bfc2d4", fontWeight: 900, fontSize: 28, margin: "0 0 16px 32px", fontFamily: "Inter, Roboto, PingFang SC, sans-serif", textShadow: "0 2px 12px #3B6FF544" }}>
                {metricName.replace(/_/g, " ").toUpperCase()}
              </div>
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

