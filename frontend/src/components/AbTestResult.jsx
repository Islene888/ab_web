import React, { useEffect, useState } from "react";
import { Table, Spin, Alert, Card, Row, Col, Form, Input, DatePicker, Button, Select } from "antd";
import ReactECharts from "echarts-for-react";
import moment from "moment";

function getUpliftStats(exp, control) {
  if (!exp || !control) return { uplift: "-", ci: "-", winRate: "-" };
  const expSamples = exp.posterior_samples;
  const ctrlSamples = control.posterior_samples;
  const n = Math.min(expSamples.length, ctrlSamples.length);
  const upliftSamples = Array.from({ length: n }, (_, i) =>
    (expSamples[i] - ctrlSamples[i]) / ctrlSamples[i]
  );
  const sorted = upliftSamples.slice().sort((a, b) => a - b);
  const ciLow = sorted[Math.floor(n * 0.025)];
  const ciHigh = sorted[Math.floor(n * 0.975)];
  const winRate = (upliftSamples.filter(x => x > 0).length / n) * 100;
  const riskProb = upliftSamples.filter(x => x < 0).length / n;
  const meanDiff = Math.abs(exp.mean - control.mean);
  const risk = (riskProb * meanDiff).toFixed(4);
  return {
    uplift: ((exp.mean - control.mean) / control.mean * 100).toFixed(2),
    ci: `[${(ciLow * 100).toFixed(2)}%, ${(ciHigh * 100).toFixed(2)}%]`,
    winRate: winRate.toFixed(1),
    riskProb: (riskProb * 100).toFixed(1),
    risk
  };
}

function UpliftViolinBar({ mean, ciLow, ciHigh }) {
  const width = 180;
  const height = 60;
  let barColor = "#bbb";
  if (ciLow > 0) barColor = "#27ae60";
  else if (ciHigh < 0) barColor = "#e74c3c";
  const min = Math.min(ciLow, mean, ciHigh, 0);
  const max = Math.max(ciLow, mean, ciHigh, 0);
  const scale = (x) => ((x - min) / (max - min || 1)) * (width - 40) + 20;
  const violinPath = `
    M${scale(ciLow)},${height / 2}
    Q${scale(mean)},${height / 2 - 18} ${scale(ciHigh)},${height / 2}
    Q${scale(mean)},${height / 2 + 18} ${scale(ciLow)},${height / 2}
    Z
  `;

  return (
    <div style={{ width, height: height + 30, position: "relative", margin: "0 auto" }}>
      <svg width={width} height={height}>
        <defs>
          <linearGradient id="violinGrad" x1="0" y1="0" x2="1" y2="0">
            <stop offset="0%" stopColor={barColor} stopOpacity="0.10" />
            <stop offset="50%" stopColor={barColor} stopOpacity="0.18" />
            <stop offset="100%" stopColor={barColor} stopOpacity="0.10" />
          </linearGradient>
        </defs>
        <path d={violinPath} fill="url(#violinGrad)" stroke="none" />
        <line x1={scale(0)} x2={scale(0)} y1={height / 2 - 22} y2={height / 2 + 22} stroke="#222" strokeWidth={1.5} opacity={0.95} />
        <line x1={scale(mean)} x2={scale(mean)} y1={height / 2 - 18} y2={height / 2 + 18} stroke="#fff" strokeWidth={2} opacity={0.95} />
      </svg>
      <div style={{ position: "absolute", left: scale(0) - 16, top: height / 2 - 38, fontSize: 16, color: "#222", minWidth: 32, textAlign: "center", fontWeight: 700 }}>0%</div>
      <div style={{ position: "absolute", left: scale(ciLow) - 24, top: height / 2 + 22, fontSize: 14, color: "#888", minWidth: 40, textAlign: "center", fontWeight: 400 }}>
        {ciLow.toFixed(2)}%
      </div>
      <div style={{ position: "absolute", left: scale(ciHigh) - 24, top: height / 2 + 22, fontSize: 14, color: "#888", minWidth: 40, textAlign: "center", fontWeight: 400 }}>
        {ciHigh.toFixed(2)}%
      </div>
      <div style={{ position: "absolute", left: scale(mean) - 20, top: height / 2 + 22, fontSize: 16, color: "#e74c3c", minWidth: 40, textAlign: "center", fontWeight: 700 }}>
        {mean.toFixed(2)}%
      </div>
    </div>
  );
}

function getResultStatus({ ciLow, ciHigh, chanceToWin }) {
  if (ciLow > 0 && chanceToWin > 0.95) return "Won";
  if (ciHigh < 0 && chanceToWin < 0.05) return "Lost";
  return "Not significant";
}

const metricNameMap = {
  aov: "AOV",
  arpu: "ARPU",
  retention: "Retention",
  business: "Business",
  chat: "Chat"
};

export default function AbTestResult({ experimentName, startDate, endDate, metric, userType }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [trend, setTrend] = useState(null);

  // 只要metric变化，自动刷新接口和图表
  useEffect(() => {
    if (!experimentName || !startDate || !endDate || !metric) return;
    setLoading(true);
    setError(null);
    // 拼接 userType 参数
    const userTypeParam = userType ? `&user_type=${userType}` : '';
    fetch(`/api/${metric}_bayesian?experiment_name=${experimentName}&start_date=${startDate}&end_date=${endDate}${userTypeParam}`)
      .then(res => res.json())
      .then(res => {
        setData(res);
        setLoading(false);
      })
      .catch(err => {
        setError(err.message);
        setLoading(false);
      });
  }, [experimentName, startDate, endDate, metric, userType]);

  useEffect(() => {
    if (!experimentName || !startDate || !endDate || !metric) return;
    const userTypeParam = userType ? `&user_type=${userType}` : '';
    fetch(`/api/${metric}_trend?experiment_name=${experimentName}&start_date=${startDate}&end_date=${endDate}${userTypeParam}`)
      .then(res => res.json())
      .then(setTrend)
      .catch(() => setTrend(null));
  }, [experimentName, startDate, endDate, metric, userType]);

  if (loading) return <Spin tip="Loading..." style={{ marginTop: 80 }} />;
  if (error) return <Alert type="error" message={"Data loading failed: " + error} showIcon style={{ marginTop: 80 }} />;

  // 统一贝叶斯表格列
  const bayesianColumns = [
    { title: "Variation", dataIndex: "variation", key: "variation", align: "center", width: 300 },
    { title: "Baseline", dataIndex: "baseline", key: "baseline", align: "center", width: 300 },
    { title: "Variation", dataIndex: "experiment", key: "experiment", align: "center", width: 300 },
    { title: "Chance to Win", dataIndex: "winrate", key: "winrate", align: "center", width: 300 },
    { title: "Credible Interval", dataIndex: "ci", key: "ci", align: "center", width: 400,
      render: (v, row) => (
        <UpliftViolinBar
          mean={Number(row.upliftNum)}
          ciLow={Number(row.ciLow) * 100}
          ciHigh={Number(row.ciHigh) * 100}
        />
      )
    },
    { title: "% Change", dataIndex: "uplift", key: "uplift", align: "center", width: 300,
      render: v => <span style={{ color: v > 0 ? '#27ae60' : '#e74c3c', fontWeight: 700 }}>{v > 0 ? '+' : ''}{v}%</span>
    },
    { title: "Risk", dataIndex: "risk", key: "risk", align: "center", width: 300 },
    {
      title: "Result",
      dataIndex: "result",
      key: "result",
      align: "center",
      width: 200,
      render: (text) => {
        let color = "#bfbfbf";
        if (text === "Won") color = "#52c41a";
        if (text === "Lost") color = "#ff4d4f";
        return <span style={{ color, fontWeight: 600 }}>{text}</span>;
      }
    }
  ];

  // 新增：如果 data 是 d1/d3/d7/d15 结构，合并渲染
  const retentionDays = ["d1", "d3", "d7", "d15"];
  const retentionColors = {
    d1: '#3B6FF5',
    d3: '#27ae60',
    d7: '#FF9900',
    d15: '#e74c3c'
  };
  if (data && retentionDays.every(day => data[day])) {
    // 组装统一风格的表格数据
    let bayesianTableData = [];
    retentionDays.forEach(day => {
      const groupData = data[day];
      if (!groupData || !Array.isArray(groupData)) return;
      // 假设 group 最小的为 Baseline
      const sortedGroups = [...groupData].sort((a, b) => (a.group > b.group ? 1 : -1));
      const control = sortedGroups[0];
      const experiments = sortedGroups.slice(1);
      experiments.forEach((g, idx) => {
        const stats = getUpliftStats(g, control);
        // 组装成和tableData一致的结构
        bayesianTableData.push({
          key: `${day}_${g.group}`,
          variation: (
            <div style={{ fontWeight: 600, fontSize: 16, textAlign: "center" }}>{day.toUpperCase()} - {g.group}</div>
          ),
          baseline: (
            <div style={{ textAlign: "center" }}>
              <div style={{ fontWeight: 500, fontSize: 16 }}>{control.mean.toFixed(2)}</div>
              <div style={{ color: "#888", fontSize: 12 }}>
                {Math.round(control.numerator !== undefined ? control.numerator : control.total_revenue)} / {Math.round(control.denominator !== undefined ? control.denominator : control.total_order)}
              </div>
            </div>
          ),
          experiment: (
            <div style={{ textAlign: "center" }}>
              <div style={{ fontWeight: 500, fontSize: 16 }}>{g.mean.toFixed(2)}</div>
              <div style={{ color: "#888", fontSize: 12 }}>
                {Math.round(g.numerator !== undefined ? g.numerator : g.total_revenue)} / {Math.round(g.denominator !== undefined ? g.denominator : g.total_order)}
              </div>
            </div>
          ),
          winrate: (
            <div
              style={{
                background: stats.uplift > 0 ? "rgba(39,174,96,0.18)" : "rgba(201, 13, 45, 0.45)",
                color: stats.uplift > 0 ? "#27ae60" : "#e74c3c",
                fontWeight: 700,
                fontSize: 22,
                borderRadius: 12,
                padding: "8px 0",
                textAlign: "center",
                width: "80px",
                margin: "0 auto",
                boxShadow: `0 0 0 1px ${stats.uplift > 0 ? '#27ae60' : '#e74c3c'}22`
              }}
            >
              {stats.winRate}%
            </div>
          ),
          ci: stats.ci,
          upliftNum: Number(stats.uplift),
          ciLow: Number(stats.ci.match(/-?[\d.]+/g)[0]),
          ciHigh: Number(stats.ci.match(/-?[\d.]+/g)[1]),
          uplift: stats.uplift,
          risk: (
            <div style={{
              color: stats.riskProb > 5 ? '#e74c3c' : stats.riskProb > 1 ? '#e67e22' : '#27ae60',
              fontWeight: 700,
              fontSize: 16,
              background: stats.riskProb > 5 ? 'rgba(231,76,60,0.12)' : stats.riskProb > 1 ? 'rgba(230,126,34,0.12)' : 'rgba(39,174,96,0.12)',
              borderRadius: 8,
              padding: "4px 8px",
              display: "inline-block"
            }}>
              {stats.riskProb}%<span style={{ fontWeight: 400, fontSize: 13, marginLeft: 4 }}>(~{stats.risk}/user)</span>
            </div>
          ),
          result: getResultStatus({
            ciLow: Number(stats.ci.match(/-?[\d.]+/g)[0]),
            ciHigh: Number(stats.ci.match(/-?[\d.]+/g)[1]),
            chanceToWin: Number(stats.winRate) / 100
          })
        });
      });
    });
    // 合并趋势图数据
    const trendSeries = [];
    let allDates = [];
    retentionDays.forEach(day => {
      const trendData = trend && trend[day];
      if (!trendData || !trendData.dates) return;
      if (allDates.length === 0) allDates = trendData.dates;
      trendData.series.forEach((s, idx) => {
        trendSeries.push({
          name: `${day.toUpperCase()}-${s.variation}`,
          type: 'line',
          data: s.data,
          smooth: true,
          symbol: 'circle',
          showSymbol: false,
          lineStyle: { width: 3, type: idx === 0 ? 'solid' : 'dashed' },
          itemStyle: { color: retentionColors[day] },
        });
      });
    });
    return (
      <Row justify="center" style={{ minHeight: "100vh" }}>
        <Col xs={24}>
          <Card
            style={{ width: "100%", maxWidth: 2500, margin: "0 auto", borderRadius: 16, boxShadow: "0 4px 24px 0 rgba(0,0,0,0.06)", background: "#fff" }}
            bodyStyle={{ padding: 32 }}
          >
            <Table
              columns={bayesianColumns}
              dataSource={bayesianTableData}
              pagination={false}
              bordered
              rowKey="key"
              scroll={{ x: true }}
              style={{ borderRadius: 12, marginBottom: 32, width: "100%" }}
            />
            <h2 style={{ textAlign: "center", margin: "32px 0 16px 0" }}>Daily Data</h2>
            <ReactECharts
              option={{
                tooltip: {
                  trigger: 'axis',
                  formatter: (params) => {
                    let html = params[0]?.axisValueLabel + '<br/>';
                    params.forEach(item => {
                      html += `<span style=\"display:inline-block;margin-right:8px;border-radius:10px;width:10px;height:10px;background:${item.color}\"></span>`;
                      html += `${item.seriesName}: <b>${item.data}</b><br/>`;
                    });
                    return html;
                  }
                },
                legend: { data: trendSeries.map(s => s.name) },
                xAxis: {
                  type: 'category',
                  data: allDates.slice(1).map(d => d.slice(0, 10)),
                  boundaryGap: false
                },
                yAxis: { type: 'value', name: 'Retention Rate', min: 'dataMin', max: 'dataMax' },
                series: trendSeries.map(s => ({ ...s, data: s.data.slice(1) }))
              }}
              style={{ height: 480, width: "100%" }}
              notMerge
              lazyUpdate
            />
          </Card>
        </Col>
      </Row>
    );
  }

  // 兼容 retention 返回的 d1/d3/d7/d15 结构
  let groups = data && data.groups;
  if (!groups && data && data.d1 && Array.isArray(data.d1)) {
    groups = [...(data.d1 || []), ...(data.d3 || []), ...(data.d7 || []), ...(data.d15 || [])];
  }
  if (!groups || groups.length < 2) return (
    <Row justify="center" style={{ minHeight: "100vh" }}>
      <Col xs={24}>
        <Card style={{ width: "100%", maxWidth: 3000, margin: "0 auto", borderRadius: 16, boxShadow: "0 4px 24px 0 rgba(0,0,0,0.06)", background: "#fff" }} bodyStyle={{ padding: 32 }}>
          <Alert type="info" message="No data available" showIcon />
        </Card>
      </Col>
    </Row>
  );

  // 假设 group 最小的为 Baseline
  const sortedGroups = [...groups].sort((a, b) => (a.group > b.group ? 1 : -1));
  const control = sortedGroups[0];
  const experiments = sortedGroups.slice(1);

  // 构造表格数据
  const tableData = experiments.map((g, idx) => {
    const stats = getUpliftStats(g, control);
    const upliftNum = Number(stats.uplift);
    const isPositive = upliftNum > 0;
    const mainColor = isPositive ? "#27ae60" : "#e74c3c";
    const bgColor = isPositive ? "rgba(39,174,96,0.18)" : "rgba(201, 13, 45, 0.45)";
    let riskColor = "#888", riskBg = "rgba(136,136,136,0.12)";
    if (stats.riskProb > 5) { riskColor = "#e74c3c"; riskBg = "rgba(231,76,60,0.12)"; }
    else if (stats.riskProb > 1) { riskColor = "#e67e22"; riskBg = "rgba(230,126,34,0.12)"; }
    else if (stats.riskProb > 0) { riskColor = "#27ae60"; riskBg = "rgba(39,174,96,0.12)"; }
    return {
      key: g.group,
      variation: (<div style={{ fontWeight: 600, fontSize: 16, textAlign: "center" }}>Variation {idx + 1}</div>),
      baseline: (
        <div style={{ textAlign: "center" }}>
          <div style={{ fontWeight: 500, fontSize: 16 }}>{control.mean.toFixed(2)}</div>
          <div style={{ color: "#888", fontSize: 12 }}>
            {Math.round(control.total_revenue)} / {Math.round(control.total_order)}
          </div>
        </div>
      ),
      experiment: (
        <div style={{ textAlign: "center" }}>
          <div style={{ fontWeight: 500, fontSize: 16 }}>{g.mean.toFixed(2)}</div>
          <div style={{ color: "#888", fontSize: 12 }}>
            {Math.round(g.total_revenue)} / {Math.round(g.total_order)}
          </div>
        </div>
      ),
      winrate: (
        <div
          style={{
            background: bgColor,
            color: "#fff",
            fontWeight: 700,
            fontSize: 22,
            borderRadius: 12,
            padding: "8px 0",
            textAlign: "center",
            width: "80px",
            margin: "0 auto",
            boxShadow: `0 0 0 1px ${mainColor}22`
          }}
        >
          {stats.winRate}%
        </div>
      ),
      ci: (
        <UpliftViolinBar
          mean={Number(stats.uplift)}
          ciLow={Number(stats.ci.match(/-?[\d.]+/g)[0]) * 100}
          ciHigh={Number(stats.ci.match(/-?[\d.]+/g)[1]) * 100}
        />
      ),
      uplift: (
        <span style={{ color: mainColor, fontWeight: 700, fontSize: 20 }}>
          {upliftNum > 0 ? "+" : ""}{stats.uplift}%
        </span>
      ),
      risk: (
        <div style={{
          color: riskColor,
          fontWeight: 700,
          fontSize: 16,
          background: riskBg,
          borderRadius: 8,
          padding: "4px 8px",
          display: "inline-block"
        }}>
          {stats.riskProb}%<span style={{ fontWeight: 400, fontSize: 13, marginLeft: 4 }}>(~{stats.risk}/user)</span>
        </div>
      ),
      result: getResultStatus({
        ciLow: Number(stats.ci.match(/-?[\d.]+/g)[0]),
        ciHigh: Number(stats.ci.match(/-?[\d.]+/g)[1]),
        chanceToWin: Number(stats.winRate) / 100
      })
    };
  });

  // 构造列
  const columns = [
    { title: "Variation", dataIndex: "variation", key: "variation", align: "center", width: 300 },
    { title: "Baseline", dataIndex: "baseline", key: "baseline", align: "center", width: 300 },
    { title: "Variation", dataIndex: "experiment", key: "experiment", align: "center", width: 300 },
    { title: "Chance to Win", dataIndex: "winrate", key: "winrate", align: "center", width: 300 },
    { title: "Credible Interval", dataIndex: "ci", key: "ci", align: "center", width: 400 },
    { title: "% Change", dataIndex: "uplift", key: "uplift", align: "center", width: 300 },
    { title: "Risk", dataIndex: "risk", key: "risk", align: "center", width: 300 },
    {
      title: "Result",
      dataIndex: "result",
      key: "result",
      align: "center",
      width: 200,
      render: (text) => {
        let color = "#bfbfbf";
        if (text === "Won") color = "#52c41a";
        if (text === "Lost") color = "#ff4d4f";
        return <span style={{ color, fontWeight: 600 }}>{text}</span>;
      }
    }
  ];

  return (
    <Row justify="center" style={{ minHeight: "100vh" }}>
      <Col xs={24}>
        <Card
          style={{ width: "100%", maxWidth: 2500, margin: "0 auto", borderRadius: 16, boxShadow: "0 4px 24px 0 rgba(0,0,0,0.06)", background: "#fff" }}
          bodyStyle={{ padding: 32 }}
        >
          <div style={{ width: "100%" }}>
            <Table
              columns={columns}
              dataSource={tableData}
              pagination={false}
              bordered
              rowKey="key"
              scroll={{ x: true }}
              style={{ borderRadius: 12, marginBottom: 32, width: "100%" }}
            />
          </div>
          {trend && trend.dates && trend.series && (
            <div style={{ width: "100%", maxWidth: 2500, margin: "24px auto 0 auto" }}>
              <h3 style={{ textAlign: "center" }}>Daily Data</h3>
              {(() => {
                const filteredDates = trend.dates.slice(1);
                const filteredSeries = trend.series.map(s => ({
                  ...s,
                  data: s.data.slice(1)
                }));
                return (
                  <ReactECharts
                    option={{
                      tooltip: {
                        trigger: 'axis',
                        formatter: (params) => {
                          let html = params[0]?.axisValueLabel + '<br/>';
                          params.forEach(item => {
                            const group = filteredSeries[item.seriesIndex];
                            const revenueArr = group.revenue || [];
                            const orderArr = group.order || [];
                            const idx = item.dataIndex;
                            const revenue = revenueArr && revenueArr[idx] !== undefined ? Math.round(revenueArr[idx]) : '-';
                            const order = orderArr && orderArr[idx] !== undefined ? Math.round(orderArr[idx]) : '-';
                            html += `<span style=\"display:inline-block;margin-right:8px;border-radius:10px;width:10px;height:10px;background:${item.color}\"></span>`;
                            html += `${item.seriesName}: <b>${item.data}</b> <span style='color:#888'>(${revenue} / ${order})</span><br/>`;
                          });
                          return html;
                        }
                      },
                      legend: { data: filteredSeries.map(s => s.variation) },
                      xAxis: {
                        type: 'category',
                        data: filteredDates.map(d => d.slice(0, 10)),
                        boundaryGap: false
                      },
                      yAxis: { type: 'value', name: metricNameMap[metric] || metric },
                      series: filteredSeries.map((s, idx) => ({
                        name: s.variation,
                        type: 'line',
                        data: s.data,
                        smooth: true,
                        symbol: 'circle',
                        showSymbol: false,
                        lineStyle: { width: 3 },
                        itemStyle: { color: idx === 0 ? '#3B6FF5' : '#FF9900' },
                      }))
                    }}
                    style={{ height: 320, width: "100%" }}
                    notMerge
                    lazyUpdate
                  />
                );
              })()}
            </div>
          )}
        </Card>
      </Col>
    </Row>
  );
}
