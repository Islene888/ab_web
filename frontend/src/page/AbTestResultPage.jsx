import React, { useState, useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Spin, message } from 'antd';
import SearchForm from '../components/AbTest/SearchForm';
import { AbTestApiProcess } from '../components/Item/Bayesian/AbTestApiProcess'; // 负责主表格API和内容展示
import AbTestTrendChart from '../components/Item/TrendChart/AbTestTrendChart';     // 负责趋势图展示

export default function AbTestResultPage() {
  const [params, setParams] = useState(null);
  const [loading, setLoading] = useState(false);
  const [searchParams] = useSearchParams();

  // 使用 useMemo 避免每次渲染都重新计算初始值
  const initialValues = useMemo(() => ({
    experiment: searchParams.get('experiment') || '',
    phase: Number(searchParams.get('phase') || 0),
  }), [searchParams]);

  /**
   * 处理表单搜索事件
   * @param {object} values - 从 SearchForm 提交的表单值
   */
  const handleSearch = async (values) => {
    // values 的结构: { experimentName, phase, daterange, category, metric }
    if (!values.daterange || values.daterange.length < 2) {
      message.error('请选择完整的日期区间');
      return;
    }
    setLoading(true);

    // 核心逻辑：根据用户的指标选择来决定调用模式 (mode)
    let mode = 'single'; // 默认是单指标模式
    let metricForApi = values.metric; // 默认使用表单的指标值

    // 假设 "All Metrics" 选项的值是 'all'
    if (Array.isArray(values.metric) && values.metric.includes('all')) {
      mode = 'all';
      // 在 'all' 模式下，具体的 metric 名称不重要，因为后端会根据 category 查询
      metricForApi = 'all';
    } else if (Array.isArray(values.metric) && values.metric.length > 0) {
      // 如果用户选择了多个指标（但不是'all'），我们默认只处理第一个
      // 或者你可以根据业务需求调整此逻辑
      metricForApi = values.metric[0];
    } else if (!Array.isArray(values.metric)) {
      metricForApi = values.metric;
    }

    // 更新 state，触发子组件的重新渲染和数据获取
    setParams({
      experimentName: values.experimentName,
      startDate: values.daterange[0].format('YYYY-MM-DD'),
      endDate: values.daterange[1].format('YYYY-MM-DD'),
      category: values.category,
      metric: metricForApi,
      mode: mode, // 将计算好的 mode 传下去
    });

    // 这里可以很快设置 loading 为 false, 因为实际的 loading 在子组件内部
    setLoading(false);
  };

  return (
    <div style={{ background: '#18192a', minHeight: '100vh', padding: 32 }}>
      <h1 style={{ color: '#fff', textAlign: 'center', fontWeight: 900, fontSize: 38, marginBottom: 32 }}>
        AB Test Results
      </h1>

      {/* 搜索表单，负责接收用户输入 */}
      <SearchForm
        onSearch={handleSearch}
        initialExperiment={initialValues.experiment}
        initialPhaseIdx={initialValues.phase}
      />

      {/* 页面级别的 Loading，用于快速反馈 */}
      {loading && <Spin style={{ display: 'block', margin: '40px auto' }} />}

      {/* 只有在用户搜索后（params 不为 null）才渲染结果区域 */}
      {!loading && params && (
        <>
          {/* 表格容器组件（API + 渲染表格） */}
          <div style={{ marginTop: 36 }}>
            <AbTestApiProcess
              experimentName={params.experimentName}
              startDate={params.startDate}
              endDate={params.endDate}
              metric={params.metric}
              category={params.category}
              mode={params.mode} // 将 mode 传递给容器组件
            />
          </div>

          {/* 分割横线 */}
          <div style={{ width: "100%", height: 2, background: "#fff", opacity: 0.11, margin: "24px 0", borderRadius: 2 }} />

          {/* 趋势图组件 */}
          <div>
            <AbTestTrendChart
              experimentName={params.experimentName}
              startDate={params.startDate}
              endDate={params.endDate}
              // 趋势图通常只针对一个指标，所以我们取第一个
              metric={Array.isArray(params.metric) ? params.metric[0] : params.metric}
            />
          </div>
        </>
      )}
    </div>
  );
}
