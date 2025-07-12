import React, { useState, useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Spin, message } from 'antd';
import SearchForm from '../components/SearchTable/SearchForm';
import { AbTestApiProcess } from '../components/Item/Bayesian/AbTestApiProcess';
import { AbTestBayesianList } from '../components/Item/Bayesian/AbTestBayesianList';
import { AbTestTrendChart } from '../components/Item/TrendChart/Render/AbTestTrendChart';
import AbTestTrendChartList from '../components/Item/TrendChart/AbTestTrendChartList';
// 引入你的API方法
import { fetchAllInOneBayesian } from '../api/abtestApi';

// ========================
// 类别与指标的映射关系
const CATEGORY_METRIC_MAP = {
  business: ["aov", "arpu", "arppu", "subscribe_rate", "payment_rate_all", "payment_rate_new", "ltv", "cancel_sub", "first_new_sub", "recharge_rate"],
  engagement: ["regen", "continue", "conversation_reset", "edit", "follow", "message", "new_conversation", "regen"],
  retention: ["all_retention", "new_retention"],
  chat: ["click_rate", "explore_start_chat_rate", "avg_chat_rounds", "avg_start_chat_bots", "avg_click_bots", "avg_time_spent", "explore_click_rate", "explore_avg_chat_rounds"],
};
// ========================

export default function AbTestResultPage() {
  const [params, setParams] = useState(null);
  const [loading, setLoading] = useState(false);
  const [allInOneMode, setAllInOneMode] = useState(false); // 全量模式标记
  const [allInOneData, setAllInOneData] = useState(null);  // 全量数据缓存
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

    let mode = 'single';
    let metricForApi = values.metric;
    let metricsForTrend = [];

    if (Array.isArray(values.metric) && values.metric.includes('all')) {
      mode = 'all';
      metricForApi = 'all';
      // 用当前类别动态取指标
      metricsForTrend = CATEGORY_METRIC_MAP[values.category] || [];
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
      metricsForTrend,
    });

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
      });
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
        marginBottom: 32
      }}>
        AB Test Results
      </h1>

      {/* 搜索表单 */}
      <SearchForm
        onSearch={handleSearch}
        onAllSearch={handleAllSearch}
        initialExperiment={initialValues.experiment}
        initialPhaseIdx={initialValues.phase}
      />

      {loading && <Spin style={{ display: 'block', margin: '40px auto' }} />}

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
            margin: "24px 0",
            borderRadius: 2
          }} />
          <AbTestTrendChartList
            experimentName={params?.experimentName}
            startDate={params?.startDate}
            endDate={params?.endDate}
            metrics={params?.metricsForTrend || Object.keys(allInOneData)}
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
