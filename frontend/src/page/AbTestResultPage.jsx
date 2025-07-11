// src/pages/AbTestResultPage.jsx

import React, { useEffect, useState } from 'react';
import { Spin, message } from 'antd';
import { useSearchParams } from 'react-router-dom';
import SearchForm from '../components/AbTest/SearchForm';
import { GrowthBookTableDemo, TrendChart } from '../components/common/AbTestResult';

export default function AbTestResultPage() {
  const [params, setParams] = useState(null);
  const [loading, setLoading] = useState(false);

  // 从 URL 读取初始 experiment 和 phase
  const [searchParams] = useSearchParams();
  const initExp = searchParams.get('experiment') || '';
  const initPhase = Number(searchParams.get('phase') || 0);

  // 初始化 params（仅在首次有 initExp 时填入默认）
  useEffect(() => {
    if (initExp) {
      setParams({
        experimentName: initExp,
        phase: initPhase,
        category: 'business',
        metric: ['aov'],
        startDate: '',
        endDate: '',
      });
    }
  }, [initExp, initPhase]);

  // 搜索表单提交
  const handleSearch = async (values) => {
    // 校验日期区间
    if (!values.daterange || !values.daterange[0] || !values.daterange[1]) {
      message.error('请选择完整的日期区间');
      return;
    }
    setLoading(true);
    const startDate = values.daterange[0].format('YYYY-MM-DD');
    const endDate = values.daterange[1].format('YYYY-MM-DD');
    setParams({ ...values, startDate, endDate });
    setLoading(false);
  };

  return (
    <div style={{ background: '#18192a', minHeight: '100vh', padding: 32 }}>
      <h1 style={{ color: '#fff', textAlign: 'center', marginBottom: 24 }}>
        AB Test Results
      </h1>

      <SearchForm
        initialExperiment={initExp}
        initialPhaseIdx={initPhase}
        onSearch={handleSearch}
      />

      {loading && <Spin style={{ display: 'block', margin: 40 }} />}

      {!loading && params && (
        <>
          {/* mock/GrowthBook 风格表格 */}
          <div style={{ marginTop: 36 }}>
            <GrowthBookTableDemo
              experimentName={params.experimentName}
              startDate={params.startDate}
              endDate={params.endDate}
              metric={Array.isArray(params.metric) ? params.metric[0] : params.metric}
            />
          </div>

        {/* 分割横线 */}
          <div
            style={{
              width: "100%",
              height: 2,
              background: "#fff",
              opacity: 0.11,
              margin: "0px 0 12px 0",
              borderRadius: 2,
            }}
          />

          {/* 趋势图 */}
          <div style={{ marginTop: 0 }}>
            <TrendChart
              experimentName={params.experimentName}
              startDate={params.startDate}
              endDate={params.endDate}
              metric={Array.isArray(params.metric) ? params.metric[0] : params.metric}
            />
          </div>
        </>
      )}
    </div>
  );
}
