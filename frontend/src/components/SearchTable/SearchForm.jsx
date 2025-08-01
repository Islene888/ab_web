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
  const [category, setCategory] = useState(undefined);
  const [metric, setMetric] = useState(undefined);

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
      ]
    });
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
  const isCohortDisabled = !!category || !!metric;
  const isCategoryMetricDisabled = !!cohortMetric;
  const canSearch =
    (cohortMetric && !category && !metric) ||
    (!cohortMetric && category && metric);

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
      onSearch && onSearch(values);
    }
  };

  // All Search 按钮
  const handleAll = () => {
    if (!category) return;
    onAllSearch && onAllSearch({
      ...form.getFieldsValue(),
      metric: ['all'],
      category,
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

        <Form.Item>
          <Select
            placeholder="Category"
            value={category}
            onChange={val => {
              setCategory(val);
              setMetric(undefined);
              setCohortMetric(undefined);
              form.setFieldsValue({ metric: undefined });
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

        <Form.Item>
          <MetricSelector
            category={category}
            value={metric}
            onChange={val => {
              setMetric(val);
              setCohortMetric(undefined);
            }}
            metricOptionsMap={metricOptionsMap}
            disabled={!category || isCategoryMetricDisabled}
            style={{ width: 140 }}
          />
        </Form.Item>

        <Form.Item>
          <Button type="primary" htmlType="submit" disabled={!canSearch}>
            Search
          </Button>
          <Button style={{ marginLeft: 8 }} onClick={handleAll} disabled={!category}>
            All Search
          </Button>
        </Form.Item>
      </Form>
    </>
  );
}
