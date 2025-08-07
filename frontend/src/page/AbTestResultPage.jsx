import React, { useState, useMemo, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Spin, message, Card } from 'antd';
import SearchForm from '../components/SearchTable/SearchForm';
import { AbTestApiProcess } from '../components/Item/Bayesian/AbTestApiProcess';
import { AbTestBayesianList } from '../components/Item/Bayesian/AbTestBayesianList';
import { AbTestTrendChart } from '../components/Item/TrendChart/Render/AbTestTrendChart';
import AbTestTrendChartList from '../components/Item/TrendChart/AbTestTrendChartList';
import { metricOptionsMap } from '../config/metricOptionsMap';
import { fetchAllInOneBayesian } from '../api/AbtestApi';
import { fetchExperiments } from '../api/GrowthbookApi';
import CohortHeatmap from '../components/Item/Cohort/CohortHeatmap';
import dayjs from 'dayjs';

export default function AbTestResultPage() {
  const [params, setParams] = useState(null);
  const [loading, setLoading] = useState(false);
  const [allInOneMode, setAllInOneMode] = useState(false);
  const [allInOneData, setAllInOneData] = useState(null);
  const [allTrendData, setAllTrendData] = useState(null);
  const [singleTrendData, setSingleTrendData] = useState(null);
  const [searchParams] = useSearchParams();

  // Cohort
  const [cohortMetric, setCohortMetric] = useState('ltv');
  const [cohortData, setCohortData] = useState(null);
  const [cohortLoading, setCohortLoading] = useState(false);

  const initialValues = useMemo(() => ({
    experiment: searchParams.get('experiment') || '',
    phase: Number(searchParams.get('phase') || 0),
  }), [searchParams]);

  // ===== 自动加载 Cohort 渲染逻辑 START =====
  useEffect(() => {
    async function autoCohort() {
      if (!initialValues.experiment) return;
      const expList = await fetchExperiments();
      const exp = expList.find(e => e.experiment_name === initialValues.experiment);
      if (!exp) return;
      const phaseObj = exp.phases[initialValues.phase] || exp.phases[0];
      const start = dayjs(phaseObj.dateStarted);
      const end = phaseObj.dateEnded && phaseObj.dateEnded !== 'now'
        ? dayjs(phaseObj.dateEnded)
        : dayjs();
      handleCohortSearch({
        experimentName: initialValues.experiment,
        phase: initialValues.phase,
        daterange: [start, end],
        cohortMetric: cohortMetric || 'ltv'
      });
    }
    autoCohort();
    // eslint-disable-next-line
  }, [initialValues.experiment, initialValues.phase]);

  // =================== 普通/单指标/all 查询 ===================
    const handleSearch = async (values) => {

    setCohortLoading(false);
    setCohortData(null);
      // 切换到业务指标时，清空 cohort 内容
    setCohortData(null);
    setParams(null);
    setCohortLoading(false);

    setAllInOneMode(false);
    setAllInOneData(null);
    setAllTrendData(null);
    setSingleTrendData(null);

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
      try {
        const res = await fetch(
          `/api/all_trend?experiment_name=${encodeURIComponent(values.experimentName)}&start_date=${start.format('YYYY-MM-DD')}&end_date=${end.format('YYYY-MM-DD')}&metric=all&category=${values.category}`
        );
        setAllTrendData(await res.json());
      } catch {
        setAllTrendData(null);
      }
    } else {
      const list = Array.isArray(values.metric) ? values.metric : [values.metric];
      metricForApi = list[0];
      metricsForTrend = list;
      try {
        // ⭐⭐ 正确接口，只查单指标
        const res = await fetch(
          `/api/${metricForApi}_trend?experiment_name=${encodeURIComponent(values.experimentName)}&start_date=${start.format('YYYY-MM-DD')}&end_date=${end.format('YYYY-MM-DD')}&metric=${metricForApi}&category=${values.category}`
        );
        setSingleTrendData(await res.json());
      } catch {
        setSingleTrendData(null);
      }
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
    setLoading(false);
  };


  // ============== 全量 all_in_one 查询 ==============
  const handleAllSearch = async (values) => {
    // 清空 cohort
    setCohortData(null);
    setCohortLoading(false);

    setAllInOneMode(false);
    setAllInOneData(null);
    setAllTrendData(null);

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

  // =================== Cohort 查询 ===================
  const handleCohortSearch = async ({ experimentName, phase, daterange, cohortMetric }) => {

    setCohortLoading(true);    // 先转 loading
    setCohortData(null);       // 再清空数据
    // 切换到 cohort 时，清空业务分析内容
    setAllInOneMode(false);
    setAllInOneData(null);
    setAllTrendData(null);
    setSingleTrendData(null);


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
          fetch(`/api/cohort/cohort_retention_heatmap?experiment_name=${encodeURIComponent(experimentName)}&start_date=${startDate}&end_date=${endDate}`)
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

  // ============ 渲染逻辑只显示一种模式 ============
  const showCohort = params && params.cohortMetric;
  const showBusiness = params && !params.cohortMetric;

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

      <div style={{ marginTop: 32 }} />

      {/* Cohort 分析区（只显示 cohort 相关内容） */}
      {showCohort && (
      cohortLoading ? (
        // 整个页面中央只显示一个 loading
        <div style={{
          minHeight: '60vh',
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center'
        }}>
          <Spin size="large" />
        </div>
      ) : (
        // cohort 加载结束，正常渲染
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
              {cohortData && cohortData.heatmap
                ? <CohortHeatmap heatmapData={cohortData.heatmap} />
                : <div style={{ color: '#ffffff', textAlign: 'center', padding: 32 }}>暂无热力图数据</div>
              }
            </Card>
          </div>
          <div style={{ width: '100%', minHeight: 400 }}>
            <Card
              title="Calculated Trending Chart"
              headStyle={{ color: '#fff', background: '#23243a', fontWeight: 600 }}
              bodyStyle={{ background: '#23243a', borderRadius: 12 }}
              style={{ background: '#23243a', border: 0, borderRadius: 14 }}
            >
              {cohortData && cohortData.trend
                ? <AbTestTrendChart trend={cohortData.trend} metric={cohortMetric} />
                : <div style={{ color: '#fff', textAlign: 'center', padding: 32 }}>暂无趋势图数据</div>
              }
            </Card>
          </div>
        </div>
      )
    )}

      {/* 业务指标分析区（只显示业务指标内容） */}
      {showBusiness && (
        <>
          {/* allInOneMode */}
          {!loading && allInOneMode && allInOneData && (
            <>
              <AbTestBayesianList loading={false} error={null} data={allInOneData} mode="all_in_one" metric="" />
              <div style={{
                width: '100%', height: 2, background: '#fff',
                opacity: 0.11, margin: '24px auto', borderRadius: 2
              }}/>
              {allTrendData &&
                <AbTestTrendChartList
                  experimentName={params?.experimentName}
                  startDate={params?.startDate}
                  endDate={params?.endDate}
                  metrics={params?.metricsForTrend}
                  category={params?.category}
                  trendData={allTrendData}
                />
              }
            </>
          )}

          {/* 非 allInOne */}
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
                  ? (allTrendData &&
                      <AbTestTrendChartList
                        experimentName={params.experimentName}
                        startDate={params.startDate}
                        endDate={params.endDate}
                        metrics={params.metricsForTrend}
                        category={params.category}
                        trendData={allTrendData}
                      />
                    )
                  : (singleTrendData &&
                      <AbTestTrendChart
                        experimentName={params.experimentName}
                        startDate={params.startDate}
                        endDate={params.endDate}
                        metric={params.metric}
                        category={params.category}
                        trend={singleTrendData}
                      />
                    )
                }
              </div>
            </>
          )}
        </>
      )}

      {/* 全局 loading */}
      {loading && <Spin style={{ display: 'block', margin: '6px auto' }} />}
    </div>
  );
}
