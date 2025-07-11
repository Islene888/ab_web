// src/components/common/AbTestResult.jsx

import React, { useEffect, useState } from "react";
import { Table, Spin, Alert, Card, Row, Col } from "antd";
import ReactECharts from "echarts-for-react";
import { fetchBayesian, fetchAllBayesian, fetchAllInOneBayesian } from "../../api/abtestApi";


// 1. 统计工具和核函数
function getUpliftStats(exp, control) {
  if (!exp || !control) return { uplift: null, ciLow: null, ciHigh: null, winRate: null, riskProb: null, risk: null, upliftSamples: [] };
  const expSamples = exp.posterior_samples;
  const ctrlSamples = control.posterior_samples;
  const n = Math.min(expSamples.length, ctrlSamples.length);
  const upliftSamples = Array.from({ length: n }, (_, i) =>
    (expSamples[i] - ctrlSamples[i]) / ctrlSamples[i]
  );
  const sorted = upliftSamples.slice().sort((a, b) => a - b);
  const ciLow = sorted[Math.floor(n * 0.025)];
  const ciHigh = sorted[Math.floor(n * 0.975)];
  const winRate = upliftSamples.filter(x => x > 0).length / n;
  const riskProb = upliftSamples.filter(x => x < 0).length / n;
  const meanDiff = Math.abs(exp.mean - control.mean);
  const risk = (riskProb * meanDiff).toFixed(4);
  return {
    uplift: (exp.mean - control.mean) / control.mean,
    ciLow,
    ciHigh,
    winRate,
    riskProb,
    risk,
    upliftSamples
  };
}
function gaussianKernel(u) {
  return Math.exp(-0.5 * u * u) / Math.sqrt(2 * Math.PI);
}
function kde(samples, xs, bandwidth) {
  return xs.map(x =>
    samples.reduce((sum, xi) => sum + gaussianKernel((x - xi) / bandwidth), 0) / (samples.length * bandwidth)
  );
}
function getPercentTicks(min, max) {
  let minPct = Math.ceil(min * 100);
  let maxPct = Math.floor(max * 100);
  let range = maxPct - minPct;
  let step = 1;
  if (range > 10) step = 5;
  if (range > 30) step = 10;
  minPct = Math.ceil(minPct / step) * step;
  maxPct = Math.floor(maxPct / step) * step;
  let ticks = [];
  for (let v = minPct; v <= maxPct; v += step) {
    ticks.push(v);
  }
  return ticks;
}
function genMockSamples(mean, std, n = 100) {
  return Array.from({ length: n }, () => mean + std * Math.sqrt(-2 * Math.log(Math.random())) * Math.cos(2 * Math.PI * Math.random()));
}

// 2. 可视化组件
function ViolinPlot({ samples, mean, color, ticks, min, max, violinWidth = 100 }) {
  if (!Array.isArray(samples) || samples.length < 2) return null;
  if (!Array.isArray(ticks) || ticks.length < 2) return null;
  function quantile(arr, q) {
    const sorted = [...arr].sort((a, b) => a - b);
    const pos = (sorted.length - 1) * q;
    const base = Math.floor(pos);
    const rest = pos - base;
    if (sorted[base + 1] !== undefined) {
      return sorted[base] + rest * (sorted[base + 1] - sorted[base]);
    } else {
      return sorted[base];
    }
  }
  const ciLow = quantile(samples, 0.025);
  const ciHigh = quantile(samples, 0.975);
  const width = violinWidth;
  const height = 25;
  const tickMin = Math.min(...ticks) / 100;
  const tickMax = Math.max(...ticks) / 100;
  const tickRange = tickMax - tickMin || 1;
  const scaleX = x => ((x - tickMin) / tickRange) * width;
  const N = 8000;
  const xs = Array.from({ length: N }, (_, i) => ciLow + (ciHigh - ciLow) * i / (N - 1));
  const m = mean ?? (samples.reduce((a, b) => a + b, 0) / samples.length);
  const std = Math.sqrt(samples.reduce((a, b) => a + Math.pow(b - m, 2), 0) / samples.length);
  const bandwidth = std * 0.4 || 1e-6;
  const density = kde(samples, xs, bandwidth);
  const maxDensity = Math.max(...density) || 1;
  const scaleY = d => maxDensity === 0 ? 0 : (d / maxDensity) * (height / 2 * 0.9);
  density[0] = 0;
  density[density.length - 1] = 0;
  const zeroIndex = xs.findIndex(x => x >= 0);
  function buildViolinPath(xs, density, scaleX, scaleY, height) {
    let path = `M${scaleX(xs[0])},${height/2}`;
    for (let i = 0; i < xs.length; ++i) {
      path += ` L${scaleX(xs[i])},${height/2 - scaleY(density[i])}`;
    }
    path += ` L${scaleX(xs[xs.length-1])},${height/2}`;
    for (let i = xs.length-1; i >= 0; --i) {
      path += ` L${scaleX(xs[i])},${height/2 + scaleY(density[i])}`;
    }
    path += ` L${scaleX(xs[0])},${height/2} Z`;
    return path;
  }
  let leftPath = null, rightPath = null;
  if (zeroIndex > 0 && zeroIndex < xs.length-1) {
    leftPath = buildViolinPath(xs.slice(0, zeroIndex+1), density.slice(0, zeroIndex+1), scaleX, scaleY, height);
    rightPath = buildViolinPath(xs.slice(zeroIndex), density.slice(zeroIndex), scaleX, scaleY, height);
  } else {
    leftPath = buildViolinPath(xs, density, scaleX, scaleY, height);
  }
  const meanX = scaleX(mean ?? m);
  const tickTextY = -25;
  const tickLineY1 = -15;
  const tickLineY2 = height + 35;
  const isAllPositive = ciLow >= 0;
  const isAllNegative = ciHigh <= 0;
  const mainColor = isAllPositive ? "#27ae60" : isAllNegative ? "#ff5c5c" : "#ff5c5c";
  const fillColor = isAllPositive ? "#27ae60cc" : isAllNegative ? "#ff5c5ccc" : "#ff5c5ccc";
  return (
    <svg
      width={width}
      height={height + 35}
      viewBox={`0 0 ${width} ${height + 24}`}
      style={{ display: "block", margin: 0, padding: 0, height: "100%", overflow: "visible" }}
    >
      {Array.isArray(ticks) && ticks.map((tick, i) => {
        const x = scaleX(tick / 100);
        return (
          <g key={i}>
            <text
              x={x}
              y={tickTextY}
              textAnchor="middle"
              fontSize={13}
              fontWeight={700}
              fill="#e6eaf7"
              style={{ userSelect: 'none' }}
            >{tick}%</text>
            <line
              x1={x}
              x2={x}
              y1={tickLineY1}
              y2={tickLineY2}
              stroke="#e6eaf7"
              strokeWidth={1}
              opacity={0.4}
            />
          </g>
        );
      })}
      {leftPath && <path d={leftPath} fill={fillColor} stroke={mainColor} strokeWidth={2} />}
      {rightPath && !isAllPositive && !isAllNegative && <path d={rightPath} fill="#27ae60cc" stroke="#27ae60" strokeWidth={2} />}
      <line x1={meanX} x2={meanX} y1={height / 2 - height / 2 * 0.9} y2={height / 2 + height / 2 * 0.9} stroke="#fff" strokeWidth={2} opacity={1} />
    </svg>
  );
}



// 主业务组件，包含三种模式
export function GrowthBookTableDemo(props) {
  const {
    experimentName = "",
    startDate = "",
    endDate = "",
    metric = "",
    category = "",
    mode = "single", // "single" | "all" | "all_in_one"
    data: propData,
  } = props;

  const [data, setData] = useState(propData || null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!experimentName || !startDate || !endDate) return;
    setLoading(true);
    setError(null);

    let fetchPromise;
    if (mode === "all") {
      fetchPromise = fetchAllBayesian({ experimentName, startDate, endDate, category });
    } else if (mode === "all_in_one") {
      fetchPromise = fetchAllInOneBayesian({ experimentName, startDate, endDate, category });
    } else {
      fetchPromise = fetchBayesian({ experimentName, startDate, endDate, metric });
    }

    fetchPromise
      .then((res) => {
        setData(res);
        setLoading(false);
      })
      .catch((err) => {
        setError(err.message || "Error fetching data");
        setLoading(false);
      });
  }, [experimentName, startDate, endDate, metric, category, mode]);

  // 多指标递归渲染
  if (mode === "all" || mode === "all_in_one") {
    if (loading) return <Spin style={{ margin: 40 }} />;
    if (error) return <Alert type="error" message={error} showIcon />;
    if (!data || Object.keys(data).length === 0)
      return (
        <div style={{ color: "#fff", textAlign: "center", padding: 32 }}>
          暂无多指标数据
        </div>
      );
    return (
      <div>
        {Object.entries(data).map(([metricName, metricData]) => (
          <div
            key={metricName}
            style={{ margin: "48px auto", maxWidth: 1400, width: "98%" }}
          >
            <div
              style={{
                color: "#bfc2d4",
                fontWeight: 900,
                fontSize: 28,
                letterSpacing: 1,
                margin: "0 0 16px 32px",
                fontFamily: "Inter, Roboto, PingFang SC, sans-serif",
                textShadow: "0 2px 12px #3B6FF544",
              }}
            >
              {metricName.replace(/_/g, " ").toUpperCase()}
            </div>
            <GrowthBookTableDemo
              mode="single"
              data={metricData.groups || metricData}
              experimentName={experimentName}
              startDate={startDate}
              endDate={endDate}
              metric={metricName}
            />
          </div>
        ))}
      </div>
    );
  }

  // ---- 单指标渲染 ----
  // 你之前的单指标表格渲染代码如下
  const groups = propData || (data && data.groups) || [];
  const bg = "#23243a";
  const border = "#2d2f4a";
  const thColor = "#dbe2f9";
  const mainFont = "#fff";
  const subFont = "#7c819a";
  const red = "#ff5c5c";
  const green = "#27ae60";
  const gray = "#888";
  const highlight = "#3a2233";
  const formatCompact = (n) =>
    typeof n === "number" && !isNaN(n)
      ? n.toLocaleString("en-US", { notation: "compact", maximumFractionDigits: 1 })
      : n === 0
      ? "0"
      : "-";
  const violinWidth = 100;

  // ticks 等可视化参数
  let ticks = [],
    min = -0.02,
    max = 0.02;
  const firstViolin = (Array.isArray(groups) ? groups : []).find(
    (row) =>
      row.violinData &&
      Array.isArray(row.violinData.samples) &&
      row.violinData.samples.length > 1
  );
  if (firstViolin && firstViolin.violinData) {
    const samples = firstViolin.violinData.samples;
    if (samples && samples.length > 1) {
      const sampleMin = Math.min(...samples);
      const sampleMax = Math.max(...samples);
      ticks = getPercentTicks(sampleMin, sampleMax);
      min = ticks[0] / 100;
      max = ticks[ticks.length - 1] / 100;
    }
  }

  let control, experiments;
  if (groups && groups.length >= 2) {
    const sortedGroups = [...groups].sort((a, b) => (a.group > b.group ? 1 : -1));
    control = sortedGroups[0];
    experiments = sortedGroups.slice(1);
  } else {
    control = null;
    experiments = [];
  }
  const tableData = (experiments || []).map((g, idx) => {
    const stats = getUpliftStats(g, control);
    const mean = stats.uplift;
    const violinData = {
      mean: stats.uplift,
      ciLow: stats.ciLow,
      ciHigh: stats.ciHigh,
      samples: stats.upliftSamples,
      winRate: stats.winRate,
    };
    const chance = typeof stats.winRate === "number" ? stats.winRate : null;
    let result = null;
    if (
      typeof stats.ciLow === "number" &&
      typeof stats.ciHigh === "number" &&
      typeof chance === "number"
    ) {
      if (stats.ciLow > 0 && chance > 0.95) result = "Won";
      else if (stats.ciHigh < 0 && chance < 0.05) result = "Lost";
      else result = "Not significant";
    }
    return {
      key: g.group || idx,
      name: `Group ${g.group ?? idx}`,
      baseline: {
        pct: control.mean / 100,
        num: control.total_revenue,
        den: control.total_order,
      },
      variation: {
        pct: g.mean / 100,
        num: g.total_revenue,
        den: g.total_order,
      },
      chance,
      violin: true,
      violinData,
      pctChange: mean,
      result,
    };
  });

  if (loading) return <Spin style={{ margin: 40 }} />;
  if (error) return <Alert type="error" message={error} showIcon />;
  if (!control || !Array.isArray(tableData) || tableData.length === 0)
    return (
      <div style={{ color: "#fff", textAlign: "center", padding: 32 }}>
        暂无有效数据
      </div>
    );

  return (
    <div
      style={{
        background: bg,
        borderRadius: 0,
        boxShadow: "0 6px 32px 0 rgba(0,0,0,0.13)",
        maxWidth: "100%",
        margin: "0 auto",
        padding: 0,
        overflowX: "auto",
      }}
    >
      {metric && (
        <div
          style={{
            textAlign: "left",
            color: "#bfc2d4",
            fontWeight: 900,
            fontSize: 22,
            letterSpacing: 1,
            margin: "0 0 8px 32px",
            fontFamily: "Inter, Roboto, PingFang SC, sans-serif",
            textShadow: "0 2px 12px #3B6FF544",
          }}
        >
          Metrics:{" "}
          {typeof metric === "string"
            ? metric.toUpperCase()
            : Array.isArray(metric)
            ? metric.map((m) => m.toUpperCase()).join(", ")
            : ""}
        </div>
      )}
      <table
        style={{
          width: "100%",
          minWidth: 1100,
          borderCollapse: "collapse",
        }}
      >
        <thead>
          <tr
            style={{
              color: thColor,
              fontWeight: 800,
              fontSize: 15,
              borderBottom: `2.5px solid ${border}`,
            }}
          >
            <th
              style={{
                textAlign: "left",
                padding: "16px 0 16px 32px",
                fontWeight: 800,
                minWidth: 240,
                borderRight: `2px solid ${border}`,
                fontSize: 15,
                color: thColor,
              }}
            >
              Group
            </th>
            <th
              style={{
                textAlign: "left",
                fontWeight: 800,
                fontSize: 15,
                minWidth: 140,
                borderRight: `2px solid ${border}`,
              }}
            >
              Baseline
            </th>
            <th
              style={{
                textAlign: "left",
                fontWeight: 800,
                fontSize: 15,
                minWidth: 140,
                borderRight: `2px solid ${border}`,
              }}
            >
              Variation
            </th>
            <th
              style={{
                textAlign: "left",
                fontWeight: 800,
                fontSize: 15,
                minWidth: 140,
                borderRight: `2px solid ${border}`,
              }}
            >
              Chance to Win
            </th>
            <th
              style={{
                textAlign: "center",
                padding: 0,
                background: "transparent",
                minWidth: 260,
                borderRight: `2px solid ${border}`,
              }}
            >
              <div style={{ height: 44 }} />
            </th>
            <th
              style={{
                textAlign: "left",
                fontWeight: 800,
                minWidth: 120,
                borderRight: `2px solid ${border}`,
              }}
            >
              % Change
            </th>
            <th
              style={{
                textAlign: "left",
                fontWeight: 800,
                minWidth: 120,
              }}
            >
              Result
            </th>
          </tr>
        </thead>
        <tbody>
          {tableData.map((row, idx) => (
            <tr
              key={row.key}
              style={{
                borderBottom:
                  idx === tableData.length - 1 ? "none" : `2px solid ${border}`,
                height: 68,
              }}
            >
              <td
                style={{
                  textAlign: "left",
                  color: mainFont,
                  fontWeight: 700,
                  fontSize: 18,
                  padding: "22px 0 22px 32px",
                  verticalAlign: "top",
                  minWidth: 240,
                  borderRight: `2px solid ${border}`,
                }}
              >
                {row.name}
              </td>
              <td
                style={{
                  textAlign: "left",
                  verticalAlign: "middle",
                  minWidth: 140,
                  borderRight: `2px solid ${border}`,
                  paddingLeft: 0,
                }}
              >
                <div
                  style={{ fontWeight: 800, fontSize: 18, color: mainFont }}
                >
                  {(row.baseline.pct * 100).toFixed(4)}%
                </div>
                <div
                  style={{
                    color: "#a0a4b8",
                    fontSize: 12,
                    fontStyle: "italic",
                    fontWeight: 500,
                    marginTop: 2,
                  }}
                >
                  {formatCompact(row.baseline.num)} / {formatCompact(row.baseline.den)}
                </div>
              </td>
              <td
                style={{
                  textAlign: "left",
                  verticalAlign: "middle",
                  minWidth: 140,
                  borderRight: `2px solid ${border}`,
                  paddingLeft: 0,
                }}
              >
                <div
                  style={{ fontWeight: 800, fontSize: 18, color: mainFont }}
                >
                  {(row.variation.pct * 100).toFixed(4)}%
                </div>
                <div
                  style={{
                    color: "#a0a4b8",
                    fontSize: 12,
                    fontStyle: "italic",
                    fontWeight: 500,
                    marginTop: 2,
                  }}
                >
                  {formatCompact(row.variation.num)} / {formatCompact(row.variation.den)}
                </div>
              </td>
              <td
                style={{
                  textAlign: "left",
                  verticalAlign: "middle",
                  minWidth: 140,
                  borderRight: `2px solid ${border}`,
                  background:
                    row.chance === null
                      ? "transparent"
                      : row.chance === 0
                      ? "#3a2233"
                      : row.chance > 0.5
                      ? "#0e3c3c"
                      : "#3a2233",
                  paddingLeft: 0,
                }}
              >
                {row.chance === null ? (
                  <span
                    style={{
                      color: gray,
                      fontStyle: "italic",
                      fontWeight: 600,
                      fontSize: 18,
                    }}
                  >
                    no data
                  </span>
                ) : (
                  <span
                    style={{
                      fontWeight: 800,
                      fontSize: 15,
                      color:
                        row.chance === 0
                          ? mainFont
                          : row.chance > 0.5
                          ? green
                          : red,
                      borderRadius: 0,
                      padding: "6px 18px 6px 0",
                      display: "inline-block",
                      minWidth: 60,
                      boxShadow: "none",
                    }}
                  >
                    {row.chance === 0
                      ? "0.0%"
                      : `${(row.chance * 100).toFixed(1)}%`}
                  </span>
                )}
              </td>
              <td
                style={{
                  textAlign: "center",
                  verticalAlign: "middle",
                  minWidth: 80,
                  maxWidth: 100,
                  width: 100,
                  padding: 0,
                  borderRight: `2px solid ${border}`,
                }}
              >
                <div
                  style={{
                    display: "flex",
                    justifyContent: "center",
                    alignItems: "center",
                    width: "100%",
                    maxWidth: 100,
                    margin: "0 auto",
                  }}
                >
                  {row.violinData
                    ? (() => {
                        let samples = row.violinData.samples;
                        if (!samples || samples.length <= 1) {
                          if (
                            typeof row.violinData.mean === "number" &&
                            typeof row.violinData.ciLow === "number" &&
                            typeof row.violinData.ciHigh === "number"
                          ) {
                            const mean = row.violinData.mean;
                            const std =
                              Math.abs(row.violinData.ciHigh - row.violinData.ciLow) / 4;
                            samples = genMockSamples(mean, std, 100);
                          }
                        }
                        if (samples && samples.length > 1) {
                          const sampleMin = Math.min(...samples);
                          const sampleMax = Math.max(...samples);
                          const ticks = getPercentTicks(sampleMin, sampleMax);
                          const min = ticks[0] / 100;
                          const max = ticks[ticks.length - 1] / 100;
                          return (
                            <ViolinPlot
                              samples={samples}
                              mean={row.violinData.mean}
                              color={row.violinData.mean >= 0 ? green : red}
                              ticks={ticks}
                              min={min}
                              max={max}
                              violinWidth={violinWidth}
                            />
                          );
                        }
                        return (
                          <span
                            style={{
                              color: "#888",
                              fontStyle: "italic",
                              fontSize: 14,
                            }}
                          >
                            无置信区间数据
                          </span>
                        );
                      })()
                    : null}
                </div>
              </td>
              <td
                style={{
                  textAlign: "left",
                  verticalAlign: "middle",
                  fontWeight: 900,
                  fontSize: 18,
                  whiteSpace: "nowrap",
                  minWidth: 120,
                  borderRight: `2px solid ${border}`,
                }}
              >
                {row.pctChange === null ? null : (
                  <span
                    style={{
                      color: row.pctChange < 0 ? red : green,
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "flex-start",
                      gap: 2,
                    }}
                  >
                    <span style={{ fontSize: 20, fontWeight: 900 }}>
                      {row.pctChange < 0 ? "↓" : "↑"}
                    </span>
                    <span style={{ fontSize: 16, fontWeight: 900 }}>
                      {(row.pctChange * 100).toFixed(2)}%
                    </span>
                  </span>
                )}
              </td>
              <td
                style={{
                  textAlign: "left",
                  verticalAlign: "middle",
                  minWidth: 120,
                  fontWeight: 800,
                }}
              >
                <span
                  className={
                    row.result === "Lost"
                      ? "ab-result-lost"
                      : row.result === "Won"
                      ? "ab-result-won"
                      : "ab-result-not"
                  }
                >
                  {row.result}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// 你也可以将 TrendChart 保持原有实现，如需多指标支持可私聊

export function TrendChart({ experimentName, startDate, endDate, metric }) {
  const [trend, setTrend] = React.useState(null);

  React.useEffect(() => {
    if (!experimentName || !startDate || !endDate || !metric) return;
    fetch(`/api/${metric}_trend?experiment_name=${experimentName}&start_date=${startDate}&end_date=${endDate}`)
      .then(res => res.json())
      .then(setTrend)
      .catch(() => setTrend(null));
  }, [experimentName, startDate, endDate, metric]);

  if (!trend || !trend.dates || !trend.series) return null;

  const filteredDates = trend.dates;
  const filteredSeries = trend.series.filter(s =>
    Array.isArray(s.data) &&
    s.data.every(v => v === null || v === undefined || v >= 0) &&
    s.data.some(v => v > 0)
  );
  const allData = [].concat(...filteredSeries.map(s => s.data)).filter(v => typeof v === 'number');
  let minData = Math.min(...allData);
  let maxData = Math.max(...allData);
  if (minData === maxData) {
    minData = minData * 0.9;
    maxData = maxData * 1.1;
  } else {
    const padding = (maxData - minData) * 0.1;
    minData = minData - padding;
    maxData = maxData + padding;
  }
  if (Math.abs(maxData) < 1e-6 && Math.abs(minData) < 1e-6) {
    return <div style={{color:'#fff',textAlign:'center',padding:32}}>暂无有效趋势数据</div>;
  }
  const bg = "#23243a";
  const cardShadow = "0 6px 32px 0 rgba(0,0,0,0.13)";
  const border = "#2d2f4a";
  const mainFont = "#fff";
  const subFont = "#7c819a";
  const thColor = "#dbe2f9";

  return (
    <div style={{ background: bg, borderRadius: 0, boxShadow: cardShadow, maxWidth: "100%", margin: "0 auto 48px auto", padding: 0 }}>
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
              data: filteredSeries.map(s => s.variation),
              textStyle: { color: thColor, fontWeight: 700, fontSize: 16 },
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
                color: thColor,
                fontWeight: 700,
                fontSize: 16,
                padding: [20, 0, 0, -40],
                align: 'left',
              },
              axisLine: { lineStyle: { color: border, width: 2 } },
              axisLabel: { color: thColor, fontWeight: 700, fontSize: 15 },
              splitLine: { show: true, lineStyle: { color: border, width: 1, type: 'dashed', opacity: 0.3 } },
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

// export { GrowthBookTableDemo, TrendChart };
