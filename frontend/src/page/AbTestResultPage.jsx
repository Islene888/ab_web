// src/pages/AbTestResultPage.jsx
import React, { useState, useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Spin, message } from 'antd';
import SearchForm from '../components/SearchTable/SearchForm';
import { AbTestApiProcess } from '../components/Item/Bayesian/AbTestApiProcess';
import { AbTestBayesianList } from '../components/Item/Bayesian/AbTestBayesianList';
import { AbTestTrendChart } from '../components/Item/TrendChart/Render/AbTestTrendChart';
import AbTestTrendChartList from '../components/Item/TrendChart/AbTestTrendChartList';
import { metricOptionsMap } from '../config/metricOptionsMap';
import { fetchAllInOneBayesian } from '../api/AbtestApi';

export default function AbTestResultPage() {
  const [params, setParams] = useState(null);
  const [loading, setLoading] = useState(false);
  const [allInOneMode, setAllInOneMode] = useState(false);
  const [allInOneData, setAllInOneData] = useState(null);
  const [allTrendData, setAllTrendData] = useState(null); // ★ 新增全量趋势数据
  const [searchParams] = useSearchParams();

  const initialValues = useMemo(() => ({
    experiment: searchParams.get('experiment') || '',
    phase: Number(searchParams.get('phase') || 0),
  }), [searchParams]);

  // 普通/单指标/all 查询
  const handleSearch = async (values) => {
    setAllInOneMode(false);
    setAllInOneData(null);

    if (!values.daterange || values.daterange.length < 2) {
      message.error('请选择完整的日期区间');
      return;
    }
    setLoading(true);

    // --- 修改开始 ---
    let mode = 'single';
    let metricForApi = values.metric;
    let metricsForTrend = [];

    // 修正后的逻辑块
    if (values.metric === 'all') { // ✅ 修正：直接判断字符串是否为 'all'
      mode = 'all';
      metricForApi = 'all'; // 这行没问题，但关键是 `mode` 的正确设置
      metricsForTrend = (metricOptionsMap[values.category] || []).map(m => m.value);
    } else {
      // 这个 else 分支可以同时处理单指标（字符串）和未来可能的多选（数组）情况
      const selectedMetrics = Array.isArray(values.metric) ? values.metric : [values.metric];
      metricForApi = selectedMetrics[0]; // 对于单指标模式的 API，只取第一个
      metricsForTrend = selectedMetrics;
    }
    // --- 修改结束 ---

    setParams({
      experimentName: values.experimentName,
      startDate: values.daterange[0].format('YYYY-MM-DD'),
      endDate: values.daterange[1].format('YYYY-MM-DD'),
      category: values.category,
      metric: metricForApi,
      mode: mode, // `mode` 会被正确地设置为 'all'
      metricsForTrend,
    });

    // ★ 主动拉取全量趋势数据
    if (mode === 'all') {
      try {
        const res = await fetch(`/api/all_trend?experiment_name=${values.experimentName}&start_date=${values.daterange[0].format('YYYY-MM-DD')}&end_date=${values.daterange[1].format('YYYY-MM-DD')}&metric=all&category=${values.category}`);
        const trendData = await res.json();
        setAllTrendData(trendData);
      } catch (e) {
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

    if (!values.daterange || values.daterange.length < 2) {
      message.error('请选择完整的日期区间');
      return;
    }
    setLoading(true);

    try {
      const data = await fetchAllInOneBayesian({
        experimentName: values.experimentName,
        startDate: values.daterange[0].format('YYYY-MM-DD'),
        endDate: values.daterange[1].format('YYYY-MM-DD'),
      });
      setAllInOneData(data);
      setAllInOneMode(true);

      setParams({
        experimentName: values.experimentName,
        startDate: values.daterange[0].format('YYYY-MM-DD'),
        endDate: values.daterange[1].format('YYYY-MM-DD'),
        metricsForTrend: Object.keys(data || {}),
        category: values.category, // all_in_one 也要带 category
      });

      // ★ 主动拉取全量趋势数据
      try {
        const res = await fetch(`/api/all_trend?experiment_name=${values.experimentName}&start_date=${values.daterange[0].format('YYYY-MM-DD')}&end_date=${values.daterange[1].format('YYYY-MM-DD')}&metric=all&category=${values.category}`);
        const trendData = await res.json();
        setAllTrendData(trendData);
      } catch {
        setAllTrendData(null);
      }
    } catch (e) {
      message.error('All in one 查询失败: ' + e.message);
    } finally {
      setLoading(false);
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
        FlowGPT  A/B  Experiment  Platform
      </h1>

      {/* 搜索表单 */}
      <SearchForm
        onSearch={handleSearch}
        onAllSearch={handleAllSearch}
        initialExperiment={initialValues.experiment}
        initialPhaseIdx={initialValues.phase}
      />

      {loading && <Spin style={{ display: 'block', margin: '6px auto' }} />}

      {/* 全量 all_in_one 渲染 */}
      {!loading && allInOneMode && allInOneData && (
        <>
          <AbTestBayesianList
            loading={false}
            error={null}
            data={allInOneData}
            mode="all_in_one"
            metric=""
          />
          <div style={{
            width: "100%",
            height: 2,
            background: "#fff",
            opacity: 0.11,
            margin: "24px auto",
            borderRadius: 2
          }} />
          <AbTestTrendChartList
            experimentName={params?.experimentName}
            startDate={params?.startDate}
            endDate={params?.endDate}
            metrics={params?.metricsForTrend || Object.keys(allInOneData)}
            category={params?.category}
            trendData={allTrendData}
          />
        </>
      )}

      {/* 普通/单指标/all 渲染 */}
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
            width: "100%",
            height: 2,
            background: "#fff",
            opacity: 0.11,
            margin: "24px 0",
            borderRadius: 2
          }} />

          <div>
            {params.mode === "all" ? (
              <AbTestTrendChartList
                experimentName={params.experimentName}
                startDate={params.startDate}
                endDate={params.endDate}
                metrics={params.metricsForTrend}
                category={params.category}
                trendData={allTrendData}
              />
            ) : (
              <AbTestTrendChart
                experimentName={params.experimentName}
                startDate={params.startDate}
                endDate={params.endDate}
                metric={Array.isArray(params.metric) ? params.metric[0] : params.metric}
                category={params.category}
              />
            )}
          </div>
        </>
      )}
    </div>
  );
}