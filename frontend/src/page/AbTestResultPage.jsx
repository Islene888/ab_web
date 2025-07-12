import React, { useState, useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Spin, message } from 'antd';
import SearchForm from '../components/AbTest/SearchForm';
import { AbTestApiProcess } from '../components/Item/Bayesian/AbTestApiProcess';
import { AbTestBayesianList } from '../components/Item/Bayesian/AbTestBayesianList';
import { AbTestTrendChart }  from '../components/Item/TrendChart/Render/AbTestTrendChart';
import AbTestTrendChartList from '../components/Item/TrendChart/AbTestTrendChartList';

export default function AbTestResultPage() {
  const [params, setParams] = useState(null);
  const [loading, setLoading] = useState(false);
  const [searchParams] = useSearchParams();

  // 假设你有所有指标的全量列表（all模式下会用到）
  const ALL_METRICS = ["aov", "arpu", "arppu", "payment_rate_all", "payment_rate_new"];

  const initialValues = useMemo(() => ({
    experiment: searchParams.get('experiment') || '',
    phase: Number(searchParams.get('phase') || 0),
  }), [searchParams]);

  const handleSearch = async (values) => {
    if (!values.daterange || values.daterange.length < 2) {
      message.error('请选择完整的日期区间');
      return;
    }
    setLoading(true);

    let mode = 'single';
    let metricForApi = values.metric;
    let metricsForTrend = [];

    if (Array.isArray(values.metric) && values.metric.includes('all')) {
      mode = 'all';
      metricForApi = 'all';
      // all模式下，渲染所有指标趋势图
      metricsForTrend = ALL_METRICS;
    } else if (Array.isArray(values.metric) && values.metric.length > 0) {
      metricForApi = values.metric[0];
      metricsForTrend = [values.metric[0]];
    } else if (!Array.isArray(values.metric)) {
      metricForApi = values.metric;
      metricsForTrend = [values.metric];
    }

    setParams({
      experimentName: values.experimentName,
      startDate: values.daterange[0].format('YYYY-MM-DD'),
      endDate: values.daterange[1].format('YYYY-MM-DD'),
      category: values.category,
      metric: metricForApi,
      mode: mode,
      metricsForTrend, // 额外传递给趋势图 list
    });

    setLoading(false);
  };

  return (
    <div style={{ background: '#18192a', minHeight: '100vh', padding: 32 }}>
      <h1 style={{
        color: '#fff',
        textAlign: 'center',
        fontWeight: 900,
        fontSize: 38,
        marginBottom: 32
      }}>
        AB Test Results
      </h1>

      {/* 搜索表单 */}
      <SearchForm
        onSearch={handleSearch}
        initialExperiment={initialValues.experiment}
        initialPhaseIdx={initialValues.phase}
      />

      {loading && <Spin style={{ display: 'block', margin: '40px auto' }} />}

      {/* 只有 params 有值才展示后续内容 */}
      {!loading && params && (
        <>
          {/* 表格数据处理和渲染 */}
          <div style={{ marginTop: 36 }}>
            <AbTestApiProcess
              experimentName={params.experimentName}
              startDate={params.startDate}
              endDate={params.endDate}
              metric={params.metric}
              category={params.category}
              mode={params.mode}
            >
              {({ loading, error, data }) => (
                <AbTestBayesianList
                  loading={loading}
                  error={error}
                  data={data}
                  mode={params.mode}
                  metric={params.metric}
                />
              )}
            </AbTestApiProcess>
          </div>

          {/* 分割横线 */}
          <div style={{
            width: "100%",
            height: 2,
            background: "#fff",
            opacity: 0.11,
            margin: "24px 0",
            borderRadius: 2
          }} />

          {/* 趋势图（支持 all/single） */}
          <div>
            {params.mode === "all" ? (
              <AbTestTrendChartList
                experimentName={params.experimentName}
                startDate={params.startDate}
                endDate={params.endDate}
                metrics={params.metricsForTrend}
              />
            ) : (
              <AbTestTrendChart
                experimentName={params.experimentName}
                startDate={params.startDate}
                endDate={params.endDate}
                metric={Array.isArray(params.metric) ? params.metric[0] : params.metric}
              />
            )}
          </div>
        </>
      )}
    </div>
  );
}
