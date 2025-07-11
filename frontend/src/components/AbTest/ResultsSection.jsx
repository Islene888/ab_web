function MetricBlock({ metricKey, groups }) {
  return (
    <div
      style={{
        margin: "48px auto",
        maxWidth: 1400,
        width: "98%"
      }}
    >
      <div
        style={{
          color: "#bfc2d4",
          fontWeight: 900,
          fontSize: 28,
          letterSpacing: 1,
          margin: '0 0 16px 32px',
          fontFamily: 'Inter, Roboto, PingFang SC, sans-serif',
          textShadow: '0 2px 12px #3B6FF544'
        }}
      >
        {metricKey.replace(/_/g, " ").toUpperCase()}
      </div>
      <GrowthBookTableDemo data={groups} metric={metricKey} />
    </div>
  );
}

export default function ResultsSection({ params, dataMap }) {
  if (!params) return null;

  // 单指标逻辑
  if (!params.isAllMetrics) {
    let safeData = [];
    if (Array.isArray(dataMap)) safeData = dataMap;
    else if (Array.isArray(dataMap?.groups)) safeData = dataMap.groups;
    else if (Array.isArray(dataMap?.data)) safeData = dataMap.data;
    const metric = Array.isArray(params.metric) ? params.metric[0] : params.metric;
    return <MetricBlock metricKey={metric} groups={safeData} />;
  }

  // 多指标逻辑
  if (!dataMap || typeof dataMap !== 'object' || Object.keys(dataMap).length === 0) {
    return <div style={{ color: "#fff", textAlign: "center", padding: 32 }}>暂无多指标数据</div>;
  }

  return (
    <>
      {Object.keys(dataMap).map((metricKey, idx) => {
        let groups = [];
        const metricData = dataMap[metricKey];
        if (Array.isArray(metricData)) groups = metricData;
        else if (Array.isArray(metricData?.groups)) groups = metricData.groups;
        else if (Array.isArray(metricData?.data)) groups = metricData.data;
        return <MetricBlock key={`${metricKey}_${idx}`} metricKey={metricKey} groups={groups} />;
      })}
    </>
  );
}
