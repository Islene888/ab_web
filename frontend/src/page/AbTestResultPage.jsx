import React, { useState, useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Spin, message, Card } from 'antd';
import SearchForm from '../components/SearchTable/SearchForm';
import { AbTestApiProcess } from '../components/Item/Bayesian/AbTestApiProcess';
import { AbTestBayesianList } from '../components/Item/Bayesian/AbTestBayesianList';
import { AbTestTrendChart } from '../components/Item/TrendChart/Render/AbTestTrendChart';
import AbTestTrendChartList from '../components/Item/TrendChart/AbTestTrendChartList';
import { metricOptionsMap } from '../config/metricOptionsMap';
import { fetchAllInOneBayesian } from '../api/AbtestApi';
import CohortHeatmap from '../components/Item/Cohort/CohortHeatmap';

export default function AbTestResultPage() {
  const [params, setParams] = useState(null);
  const [loading, setLoading] = useState(false);
  const [allInOneMode, setAllInOneMode] = useState(false);
  const [allInOneData, setAllInOneData] = useState(null);
  const [allTrendData, setAllTrendData] = useState(null);
  const [searchParams] = useSearchParams();

  // Cohort
  const [cohortMetric, setCohortMetric] = useState('ltv');
  const [cohortData, setCohortData] = useState(null);
  const [cohortLoading, setCohortLoading] = useState(false);

  const initialValues = useMemo(() => ({
    experiment: searchParams.get('experiment') || '',
    phase: Number(searchParams.get('phase') || 0),
  }), [searchParams]);

  // 普通/单指标/all 查询
  const handleSearch = async (values) => {
    setAllInOneMode(false);
    setAllInOneData(null);
    const [start, end] = values.daterange || [];
    if (!start || !end) {
      message.error('请选择完整的日期区间');
      return;
    }
    setLoading(true);
    let mode = 'single';
    let metricForApi = values.metric;
    let metricsForTrend = [];
    if (values.metric === 'all') {
      mode = 'all';
      metricForApi = 'all';
      metricsForTrend = (metricOptionsMap[values.category] || []).map(m => m.value);
    } else {
      const list = Array.isArray(values.metric) ? values.metric : [values.metric];
      metricForApi = list[0];
      metricsForTrend = list;
    }
    setParams({
      experimentName: values.experimentName,
      startDate: start.format('YYYY-MM-DD'),
      endDate: end.format('YYYY-MM-DD'),
      category: values.category,
      metric: metricForApi,
      mode,
      metricsForTrend,
    });
    if (mode === 'all') {
      try {
        const res = await fetch(
          `/api/all_trend?experiment_name=${encodeURIComponent(values.experimentName)}&start_date=${start.format('YYYY-MM-DD')}&end_date=${end.format('YYYY-MM-DD')}&metric=all&category=${values.category}`
        );
        setAllTrendData(await res.json());
      } catch {
        setAllTrendData(null);
      }
    } else {
      setAllTrendData(null);
    }
    setLoading(false);
  };

  // 全量 all_in_one 查询
  const handleAllSearch = async (values) => {
    setAllInOneMode(false);
    setAllInOneData(null);
    const [start, end] = values.daterange || [];
    if (!start || !end) {
      message.error('请选择完整的日期区间');
      return;
    }
    setLoading(true);
    try {
      const data = await fetchAllInOneBayesian({
        experimentName: values.experimentName,
        startDate: start.format('YYYY-MM-DD'),
        endDate: end.format('YYYY-MM-DD'),
      });
      setAllInOneData(data);
      setAllInOneMode(true);
      setParams({
        experimentName: values.experimentName,
        startDate: start.format('YYYY-MM-DD'),
        endDate: end.format('YYYY-MM-DD'),
        metricsForTrend: Object.keys(data),
        category: values.category,
      });
      try {
        const res = await fetch(
          `/api/all_trend?experiment_name=${encodeURIComponent(values.experimentName)}&start_date=${start.format('YYYY-MM-DD')}&end_date=${end.format('YYYY-MM-DD')}&metric=all&category=${values.category}`
        );
        setAllTrendData(await res.json());
      } catch {
        setAllTrendData(null);
      }
    } catch (e) {
      message.error('All in one 查询失败: ' + e.message);
    } finally {
      setLoading(false);
    }
  };

  // Cohort 查询：趋势+热力图分别渲染
  const handleCohortSearch = async ({ experimentName, phase, daterange, cohortMetric }) => {
    const [start, end] = daterange || [];
    if (!experimentName || !start || !end || !cohortMetric) {
      message.error('请先选择实验、日期和 Cohort 指标');
      return;
    }
    const startDate = start.format('YYYY-MM-DD');
    const endDate = end.format('YYYY-MM-DD');
    setParams({ experimentName, startDate, endDate, cohortMetric });
    setCohortLoading(true);
    setCohortData(null);
    try {
      let trendRes, heatmapRes;
      if (cohortMetric === 'ltv') {
        [trendRes, heatmapRes] = await Promise.all([
          fetch(`/api/cohort/cumulative_ltv_trend?experiment_name=${encodeURIComponent(experimentName)}&start_date=${startDate}&end_date=${endDate}`)
            .then(r => r.json()),
          fetch(`/api/cohort/arpu_heatmap?experiment_name=${encodeURIComponent(experimentName)}&start_date=${startDate}&end_date=${endDate}`)
            .then(r => r.json()),
        ]);
      } else if (cohortMetric === 'lt') {
        [trendRes, heatmapRes] = await Promise.all([
          fetch(`/api/cohort/cumulative_lt_trend?experiment_name=${encodeURIComponent(experimentName)}&start_date=${startDate}&end_date=${endDate}`)
            .then(r => r.json()),
          fetch(`/api/cohort/time_spend_heatmap?experiment_name=${encodeURIComponent(experimentName)}&start_date=${startDate}&end_date=${endDate}`)
            .then(r => r.json()),
        ]);
      } else if (cohortMetric === 'retention') {
        [trendRes, heatmapRes] = await Promise.all([
          fetch(`/api/cohort/cumulative_retention_trend?experiment_name=${encodeURIComponent(experimentName)}&start_date=${startDate}&end_date=${endDate}`)
            .then(r => r.json()),
          fetch(`/api/cohort/active_retention_d1_heatmap?experiment_name=${encodeURIComponent(experimentName)}&start_date=${startDate}&end_date=${endDate}`)
            .then(r => r.json()),
        ]);
      }
      setCohortData({ trend: trendRes, heatmap: heatmapRes });
    } catch (e) {
      message.error('Cohort 查询失败：' + e.message);
    } finally {
      setCohortLoading(false);
    }
  };

  return (
    <div style={{ background: '#18192a', minHeight: '100vh', padding: 32 }}>
      <h1 style={{
        color: '#fff',
        textAlign: 'center',
        fontWeight: 900,
        fontSize: 38,
        marginBottom: 50
      }}>
        FlowGPT A/B Experiment Platform
      </h1>

      {/* 搜索表单 */}
      <SearchForm
        onSearch={handleSearch}
        onAllSearch={handleAllSearch}
        onCohortSearch={handleCohortSearch}
        initialExperiment={initialValues.experiment}
        initialPhaseIdx={initialValues.phase}
        cohortMetric={cohortMetric}
        setCohortMetric={setCohortMetric}
      />

+     {/* 和搜索表单分离一点距离 */}
+     <div style={{ marginTop: 32 }} />

      {/* Cohort 结果：趋势+热力图并排卡片展示 */}
      <div style={{
        display: 'flex',
        flexDirection: 'column',
        gap: 24,
        alignItems: 'stretch',
        marginBottom: 36
      }}>
        <div style={{ width: '100%', minHeight: 480 }}>
          <Card
            title="Cohort Heatmap"
            headStyle={{ color: '#fff', background: '#23243a', fontWeight: 600 }}
            bodyStyle={{ background: '#23243a', borderRadius: 12 }}
            style={{ background: '#23243a', border: 0, borderRadius: 14 }}
          >
            {cohortData.heatmap
              ? <CohortHeatmap heatmapData={cohortData.heatmap} />
              : <div style={{ color: '#ffffff', textAlign: 'center', padding: 32 }}>暂无热力图数据</div>}
          </Card>
        </div>

        <div style={{ width: '100%', minHeight: 400 }}>
          <Card
            title="Calculated Trending Chart"
            headStyle={{ color: '#fff', background: '#23243a', fontWeight: 600 }}
            bodyStyle={{ background: '#23243a', borderRadius: 12 }}
            style={{ background: '#23243a', border: 0, borderRadius: 14 }}
          >
            {cohortData.trend
              ? <AbTestTrendChart trend={cohortData.trend} metric={cohortMetric} />
              : <div style={{ color: '#fff', textAlign: 'center', padding: 32 }}>暂无趋势图数据</div>}
          </Card>
        </div>
      </div>









      {/* 普通业务分析 */}
      {loading && <Spin style={{ display: 'block', margin: '6px auto' }} />}
      {!loading && allInOneMode && allInOneData && (
        <>
          <AbTestBayesianList loading={false} error={null} data={allInOneData} mode="all_in_one" metric="" />
          <div style={{
            width: '100%', height: 2, background: '#fff',
            opacity: 0.11, margin: '24px auto', borderRadius: 2
          }}/>
          <AbTestTrendChartList
            experimentName={params?.experimentName}
            startDate={params?.startDate}
            endDate={params?.endDate}
            metrics={params?.metricsForTrend}
            category={params?.category}
            trendData={allTrendData}
          />
        </>
      )}

      {!loading && !allInOneMode && params && (
        <>
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
          <div style={{
            width: '100%', height: 2, background: '#fff',
            opacity: 0.11, margin: '24px 0', borderRadius: 2
          }}/>
          <div>
            {params.mode === 'all'
              ? <AbTestTrendChartList
                  experimentName={params.experimentName}
                  startDate={params.startDate}
                  endDate={params.endDate}
                  metrics={params.metricsForTrend}
                  category={params.category}
                  trendData={allTrendData}
                />
              : <AbTestTrendChart
                  experimentName={params.experimentName}
                  startDate={params.startDate}
                  endDate={params.endDate}
                  metric={params.metric}
                  category={params.category}
                />
            }
          </div>
        </>
      )}
    </div>
  );
}
