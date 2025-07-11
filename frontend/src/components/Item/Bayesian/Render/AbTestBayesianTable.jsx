//AbTestBayesianTable.jsx

import React from "react";

// 1. 统计工具和核函数 (从原文件移动至此)
function getUpliftStats(exp, control) {
  if (!exp || !control || !exp.posterior_samples || !control.posterior_samples) {
      return { uplift: null, ciLow: null, ciHigh: null, winRate: null, riskProb: null, risk: null, upliftSamples: [] };
  }
  const expSamples = exp.posterior_samples;
  const ctrlSamples = control.posterior_samples;
  const n = Math.min(expSamples.length, ctrlSamples.length);
  if (n === 0) {
      return { uplift: null, ciLow: null, ciHigh: null, winRate: null, riskProb: null, risk: null, upliftSamples: [] };
  }
  const upliftSamples = Array.from({ length: n }, (_, i) =>
    ctrlSamples[i] === 0 ? 0 : (expSamples[i] - ctrlSamples[i]) / ctrlSamples[i]
  );
  const sorted = [...upliftSamples].sort((a, b) => a - b);
  const ciLow = sorted[Math.floor(n * 0.025)];
  const ciHigh = sorted[Math.floor(n * 0.975)];
  const winRate = upliftSamples.filter(x => x > 0).length / n;
  const riskProb = upliftSamples.filter(x => x < 0).length / n;
  const meanDiff = Math.abs(exp.mean - control.mean);
  const risk = (riskProb * meanDiff).toFixed(4);
  return {
    uplift: control.mean === 0 ? 0 : (exp.mean - control.mean) / control.mean,
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

// 2. 可视化子组件 (从原文件移动至此)
function ViolinPlot({ samples, mean, ticks, violinWidth = 100 }) {
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
    const N = 200;
    const xs = Array.from({ length: N }, (_, i) => ciLow + (ciHigh - ciLow) * i / (N - 1));
    const m = mean ?? (samples.reduce((a, b) => a + b, 0) / samples.length);
    const std = Math.sqrt(samples.reduce((a, b) => a + Math.pow(b - m, 2), 0) / samples.length);
    const bandwidth = std * 0.4 || 1e-6;
    const density = kde(samples, xs, bandwidth);
    const maxDensity = Math.max(...density) || 1;
    const scaleY = d => maxDensity === 0 ? 0 : (d / maxDensity) * (height / 2 * 0.9);

    if(density.length > 0) {
      density[0] = 0;
      density[density.length - 1] = 0;
    }

    const zeroIndex = xs.findIndex(x => x >= 0);

    function buildViolinPath(xs, density, scaleX, scaleY, height) {
        if(xs.length === 0) return "";
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
    const isAllPositive = ciLow >= 0;
    const isAllNegative = ciHigh <= 0;
    const mainColor = isAllPositive ? "#27ae60" : isAllNegative ? "#ff5c5c" : "#ff5c5c";
    const fillColor = isAllPositive ? "#27ae60cc" : isAllNegative ? "#ff5c5ccc" : "#ff5c5ccc";
    
    if (!isAllPositive && !isAllNegative && zeroIndex > 0 && zeroIndex < xs.length-1) {
        leftPath = buildViolinPath(xs.slice(0, zeroIndex+1), density.slice(0, zeroIndex+1), scaleX, scaleY, height);
        rightPath = buildViolinPath(xs.slice(zeroIndex), density.slice(zeroIndex), scaleX, scaleY, height);
    } else {
        leftPath = buildViolinPath(xs, density, scaleX, scaleY, height);
    }

    const meanX = scaleX(mean ?? m);
    const tickTextY = -25;
    const tickLineY1 = -15;
    const tickLineY2 = height + 35;

    return (
        <svg width={width} height={height + 35} viewBox={`0 -30 ${width} ${height + 60}`} style={{ overflow: "visible" }}>
          {ticks.map((tick, i) => {
            const x = scaleX(tick / 100);
            return (
              <g key={i}>
                <text x={x} y={tickTextY} textAnchor="middle" fontSize={13} fontWeight={700} fill="#e6eaf7" style={{ userSelect: 'none' }}>{tick}%</text>
                <line x1={x} x2={x} y1={tickLineY1} y2={tickLineY2} stroke="#e6eaf7" strokeWidth={1} opacity={0.4} />
              </g>
            );
          })}
          {leftPath && <path d={leftPath} fill={fillColor} stroke={mainColor} strokeWidth={2} />}
          {rightPath && <path d={rightPath} fill="#27ae60cc" stroke="#27ae60" strokeWidth={2} />}
          <line x1={meanX} x2={meanX} y1={height/2 - height/2 * 0.9} y2={height/2 + height/2 * 0.9} stroke="#fff" strokeWidth={2} opacity={1} />
        </svg>
    );
}


// 3. 主展示组件
export default function AbTestBayesianTable({ metric, data: groups }) {
  // --- 样式和渲染逻辑 ---
  const bg = "#23243a";
  const border = "#2d2f4a";
  const thColor = "#dbe2f9";
  const mainFont = "#fff";
  const subFont = "#7c819a";
  const red = "#ff5c5c";
  const green = "#27ae60";
  const gray = "#888";

  const formatCompact = (n) =>
    typeof n === "number" && !isNaN(n)
      ? n.toLocaleString("en-US", { notation: "compact", maximumFractionDigits: 1 })
      : n === 0 ? "0" : "-";
  const violinWidth = 100;
  
  // --- 数据处理 ---
  if (!groups || groups.length < 2) {
    return (
      <div style={{ color: "#fff", textAlign: "center", padding: 32, minHeight: 100 }}>
        暂无有效的对比数据
      </div>
    );
  }

  const sortedGroups = [...groups].sort((a, b) => (a.group > b.group ? 1 : -1));
  const control = sortedGroups[0];
  const experiments = sortedGroups.slice(1);

  const tableData = experiments.map((g, idx) => {
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
    if (typeof stats.ciLow === "number" && typeof stats.ciHigh === "number" && typeof chance === "number") {
      if (stats.ciLow > 0 && chance > 0.95) result = "Won";
      else if (stats.ciHigh < 0 && chance < 0.05) result = "Lost";
      else result = "Not significant";
    }
    return {
      key: g.group || idx,
      name: `Group ${g.group ?? idx}`,
      baseline: {
        pct: control.mean, // 注意：这里的 pct 应该是原始值，渲染时再 *100
        num: control.total_revenue,
        den: control.total_order,
      },
      variation: {
        pct: g.mean, // 注意：这里的 pct 应该是原始值，渲染时再 *100
        num: g.total_revenue,
        den: g.total_order,
      },
      chance,
      violinData,
      pctChange: mean,
      result,
    };
  });
  
  // --- 渲染 ---
  return (
    <div style={{ background: bg, borderRadius: 0, boxShadow: "0 6px 32px 0 rgba(0,0,0,0.13)", maxWidth: "100%", margin: "0 auto", padding: 0, overflowX: "auto" }}>
      {metric && (
        <div style={{ textAlign: "left", color: "#bfc2d4", fontWeight: 900, fontSize: 22, letterSpacing: 1, margin: "0 0 8px 32px", fontFamily: "Inter, Roboto, PingFang SC, sans-serif", textShadow: "0 2px 12px #3B6FF544" }}>
          Metrics: {metric.toUpperCase()}
        </div>
      )}
      <table style={{ width: "100%", minWidth: 1100, borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ color: thColor, fontWeight: 800, fontSize: 15, borderBottom: `2.5px solid ${border}` }}>
            <th style={{ textAlign: "left", padding: "16px 0 16px 32px", minWidth: 240, borderRight: `2px solid ${border}` }}>Group</th>
            <th style={{ textAlign: "left", paddingLeft: 8, minWidth: 140, borderRight: `2px solid ${border}` }}>Baseline</th>
            <th style={{ textAlign: "left", paddingLeft: 8, minWidth: 140, borderRight: `2px solid ${border}` }}>Variation</th>
            <th style={{ textAlign: "left", paddingLeft: 8, minWidth: 140, borderRight: `2px solid ${border}` }}>Chance to Win</th>
            <th style={{ textAlign: "center", minWidth: 260, borderRight: `2px solid ${border}` }}>% Change Distribution</th>
            <th style={{ textAlign: "left", paddingLeft: 8, minWidth: 120, borderRight: `2px solid ${border}` }}>% Change</th>
            <th style={{ textAlign: "left", paddingLeft: 8, minWidth: 120 }}>Result</th>
          </tr>
        </thead>
        <tbody>
          {tableData.map((row, idx) => {
            const ticks = getPercentTicks(row.violinData.ciLow, row.violinData.ciHigh);
            return (
              <tr key={row.key} style={{ borderBottom: idx === tableData.length - 1 ? "none" : `2px solid ${border}`, height: 68 }}>
                <td style={{ textAlign: "left", color: mainFont, fontWeight: 700, fontSize: 18, padding: "22px 0 22px 32px", verticalAlign: "top", borderRight: `2px solid ${border}` }}>
                  {row.name}
                </td>
                <td style={{ textAlign: "left", verticalAlign: "middle", paddingLeft: 8, borderRight: `2px solid ${border}` }}>
                  <div style={{ fontWeight: 800, fontSize: 18, color: mainFont }}>{(row.baseline.pct * 100).toFixed(4)}%</div>
                  <div style={{ color: "#a0a4b8", fontSize: 12, fontStyle: "italic", fontWeight: 500, marginTop: 2 }}>
                    {formatCompact(row.baseline.num)} / {formatCompact(row.baseline.den)}
                  </div>
                </td>
                <td style={{ textAlign: "left", verticalAlign: "middle", paddingLeft: 8, borderRight: `2px solid ${border}` }}>
                  <div style={{ fontWeight: 800, fontSize: 18, color: mainFont }}>{(row.variation.pct * 100).toFixed(4)}%</div>
                  <div style={{ color: "#a0a4b8", fontSize: 12, fontStyle: "italic", fontWeight: 500, marginTop: 2 }}>
                    {formatCompact(row.variation.num)} / {formatCompact(row.variation.den)}
                  </div>
                </td>
                <td style={{ textAlign: "left", verticalAlign: "middle", paddingLeft: 8, borderRight: `2px solid ${border}`, background: row.chance === null ? "transparent" : row.chance > 0.5 ? "#0e3c3c" : "#3a2233" }}>
                  {row.chance === null ? <span style={{ color: gray, fontStyle: "italic" }}>no data</span> : (
                    <span style={{ fontWeight: 800, fontSize: 15, color: row.chance > 0.5 ? green : red }}>
                      {(row.chance * 100).toFixed(1)}%
                    </span>
                  )}
                </td>
                <td style={{ textAlign: "center", verticalAlign: "middle", padding: 0, borderRight: `2px solid ${border}` }}>
                  {row.violinData.samples.length > 1 ? (
                    <ViolinPlot samples={row.violinData.samples} mean={row.violinData.mean} ticks={ticks} violinWidth={violinWidth} />
                  ) : (
                    <span style={{ color: gray, fontStyle: "italic" }}>无置信区间数据</span>
                  )}
                </td>
                <td style={{ textAlign: "left", verticalAlign: "middle", paddingLeft: 8, fontWeight: 900, fontSize: 18, borderRight: `2px solid ${border}` }}>
                  {row.pctChange !== null && (
                    <span style={{ color: row.pctChange < 0 ? red : green, display: "flex", alignItems: "center", gap: 2 }}>
                      <span style={{ fontSize: 20, fontWeight: 900 }}>{row.pctChange < 0 ? "↓" : "↑"}</span>
                      <span style={{ fontSize: 16, fontWeight: 900 }}>{(row.pctChange * 100).toFixed(2)}%</span>
                    </span>
                  )}
                </td>
                <td style={{ textAlign: "left", verticalAlign: "middle", paddingLeft: 8, fontWeight: 800 }}>
                  <span className={row.result === "Lost" ? "ab-result-lost" : row.result === "Won" ? "ab-result-won" : "ab-result-not"}>
                    {row.result}
                  </span>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}