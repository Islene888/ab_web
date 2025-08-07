// src/components/SearchTable/SearchForm.jsx

import React, { useEffect, useState } from 'react';
import { Form, Button, Spin, Select } from 'antd';
import dayjs from 'dayjs';
import ExperimentSelector from './singleItem/ExperimentSelector';
import PhaseSelector from './singleItem/PhaseSelector';
import DateRangePicker from './singleItem/DateRangePicker';
import MetricSelector from './singleItem/MetricSelector';
import { metricOptionsMap } from '../../config/metricOptionsMap';
import { fetchExperiments } from '../../api/GrowthbookApi';

const { Option } = Select;

export default function SearchForm({
  initialExperiment,
  initialPhaseIdx,
  onSearch,
  onAllSearch,
  onCohortSearch,
  cohortMetric,
  setCohortMetric
}) {
  const [form] = Form.useForm();
  const [experiments, setExperiments] = useState([]);
  const [phaseOptions, setPhaseOptions] = useState([]);
  const [loading, setLoading] = useState(true);

  // 初始化 & 自动填充 Experiment → phase → 日期
  useEffect(() => {
    fetchExperiments().then(list => {
      setExperiments(list);
      setLoading(false);
      if (initialExperiment && list.length) {
        const exp = list.find(e => e.experiment_name === initialExperiment);
        if (exp) {
          const opts = exp.phases.map((p, i) => ({
            value: i,
            ...p,
            label: `${p.name || `Phase ${i + 1}`} (${p.dateStarted.split('T')[0]}~${p.dateEnded ? p.dateEnded.split('T')[0] : 'now'})`
          }));
          setPhaseOptions(opts);
          const ph = opts[initialPhaseIdx] || opts[0];
          form.setFieldsValue({
            experimentName: exp.experiment_name,
            phase: ph.value,
            daterange: [
              ph.dateStarted ? dayjs(ph.dateStarted) : null,
              (ph.dateEnded && ph.dateEnded !== 'now') ? dayjs(ph.dateEnded) : dayjs()
            ]
          });
        }
      }
    });
  }, [initialExperiment, initialPhaseIdx, form]);

  // 切换 Experiment 时更新 phaseOptions 和日期范围
  const handleExpChange = name => {
    const exp = experiments.find(e => e.experiment_name === name);
    if (!exp) return;
    const opts = exp.phases.map((p, i) => ({
      value: i,
      ...p,
      label: `${p.name || `Phase ${i + 1}`} (${p.dateStarted.split('T')[0]}~${p.dateEnded ? p.dateEnded.split('T')[0] : 'now'})`
    }));
    setPhaseOptions(opts);
    const first = opts[0];
    form.setFieldsValue({
      phase: first.value,
      daterange: [
        first.dateStarted ? dayjs(first.dateStarted) : null,
        (first.dateEnded && first.dateEnded !== 'now') ? dayjs(first.dateEnded) : dayjs()
      ],
      // 切换实验时自动清空类别和指标
      category: undefined,
      metric: undefined
    });
    setCohortMetric(undefined); // 切换实验时也清空 cohort
  };

  // 切换 Phase 时更新日期范围
  const handlePhaseChange = idx => {
    const p = phaseOptions.find(o => o.value === idx);
    if (!p) return;
    form.setFieldsValue({
      daterange: [
        p.dateStarted ? dayjs(p.dateStarted) : null,
        (p.dateEnded && p.dateEnded !== 'now') ? dayjs(p.dateEnded) : dayjs()
      ]
    });
  };

  // Cohort 与 Category/Metric 互斥逻辑
  const watchCategory = Form.useWatch('category', form);
  const watchMetric = Form.useWatch('metric', form);
  const isCohortDisabled = !!watchCategory || !!watchMetric;
  const isCategoryMetricDisabled = !!cohortMetric;
  const canSearch =
    (cohortMetric && !watchCategory && !watchMetric) ||
    (!cohortMetric && watchCategory && watchMetric);

  // Search 按钮提交
  const handleFinish = values => {
    if (cohortMetric) {
      onCohortSearch && onCohortSearch({
        experimentName: values.experimentName,
        phase: values.phase,
        daterange: values.daterange,
        cohortMetric
      });
    } else {
      onSearch && onSearch(values); // values 里现在有 category/metric 字段
    }
  };

  // All Search 按钮
  const handleAll = () => {
    const values = form.getFieldsValue();
    if (!values.category) return;
    onAllSearch && onAllSearch({
      ...values,
      metric: 'all', // 一定是字符串 'all'
      cohortMetric: undefined,
      mode: 'category_metric'
    });
  };

  if (loading) return <Spin />;

  return (
    <>
      <style>
        {`
          .search-form-inline .ant-form-item {
            margin-right: 16px;
          }
          .search-form-inline .ant-form-item:last-child {
            margin-right: 0;
          }
        `}
      </style>
      <Form
        form={form}
        layout="inline"
        onFinish={handleFinish}
        style={{ width: '100%' }}
        className="search-form-inline"
      >
        <Form.Item name="experimentName" rules={[{ required: true }]}>
          <ExperimentSelector style={{ width: 180 }} onChange={handleExpChange} />
        </Form.Item>

        <Form.Item name="phase" rules={[{ required: true }]}>
          <PhaseSelector style={{ width: 150 }} options={phaseOptions} onChange={handlePhaseChange} />
        </Form.Item>

        <Form.Item name="daterange" rules={[{ required: true }]}>
          <DateRangePicker style={{ width: 180, minWidth: 180 }} />
        </Form.Item>

        <Form.Item>
          <Select
            placeholder="Cohort"
            value={cohortMetric}
            onChange={val => setCohortMetric(val)}
            disabled={isCohortDisabled}
            style={{ width: 120 }}
            allowClear
          >
            <Option value="ltv">LTV</Option>
            <Option value="lt">LT</Option>
            <Option value="retention">Retention</Option>
          </Select>
        </Form.Item>

        {/* Category 和 Metric 都挂载到 Form.Item */}
        <Form.Item name="category" rules={[{ required: !cohortMetric, message: '请选择 Category' }]}>
          <Select
            placeholder="Category"
            onChange={val => {
              // 切换 Category 时自动清空 Metric 和 Cohort
              form.setFieldsValue({ metric: undefined });
              setCohortMetric(undefined);
            }}
            disabled={isCategoryMetricDisabled}
            style={{ width: 120 }}
            allowClear
          >
            <Option value="business">Business</Option>
            <Option value="retention">Retention</Option>
            <Option value="engagement">Engagement</Option>
            <Option value="chat">Chat</Option>
          </Select>
        </Form.Item>

        <Form.Item name="metric" rules={[{ required: !cohortMetric, message: '请选择 Metric' }]}>
          <MetricSelector
            category={form.getFieldValue('category')}
            onChange={() => {
              setCohortMetric(undefined);
            }}
            metricOptionsMap={metricOptionsMap}
            disabled={!form.getFieldValue('category') || isCategoryMetricDisabled}
            style={{ width: 140 }}
          />
        </Form.Item>

        <Form.Item>
          <Button type="primary" htmlType="submit" disabled={!canSearch}>
            Search
          </Button>
          <Button style={{ marginLeft: 8 }} onClick={handleAll} disabled={!form.getFieldValue('category')}>
            All Search
          </Button>
        </Form.Item>
      </Form>
    </>
  );
}
