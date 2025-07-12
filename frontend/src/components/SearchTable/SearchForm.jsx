import React, { useEffect, useState } from 'react';
import { Form, Button, Spin, Select } from 'antd';
import dayjs from 'dayjs';
import ExperimentSelector from './singleItem/ExperimentSelector';
import PhaseSelector from './singleItem/PhaseSelector';
import DateRangePicker from './singleItem/DateRangePicker';
import MetricSelector from './singleItem/MetricSelector';
import {  metricOptionsMap } from '../../config/metricOptionsMap';
import { fetchExperiments } from '../../api/GrowthbookApi';

const { Option } = Select;

export default function SearchForm({ initialExperiment, initialPhaseIdx, onSearch, onAllSearch }) {
  const [form] = Form.useForm();
  const [experiments, setExperiments] = useState([]);
  const [phaseOptions, setPhaseOptions] = useState([]);
  const [loading, setLoading] = useState(true);
  const [category, setCategory] = useState('business');

  useEffect(() => {
    fetchExperiments().then(list => {
      setExperiments(list);
      setLoading(false);
      // 预填充
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
            ph.dateEnded ? dayjs(ph.dateEnded) : null
          ],
          category: 'business',
          metric: []
        });
      }
    });
  }, [initialExperiment, initialPhaseIdx, form]);

  // 切换实验
  const handleExpChange = name => {
    const exp = experiments.find(e => e.experiment_name === name);
    if (!exp) return;
    const opts = exp.phases.map((p, i) => ({
      value: i,
      ...p,
      label: `${p.name || `Phase ${i + 1}`} (${p.dateStarted.split('T')[0]}~${p.dateEnded ? p.dateEnded.split('T')[0] : 'now'})`
    }));
    setPhaseOptions(opts);
    form.setFieldsValue({
      phase: opts[0].value,
      daterange: [
        opts[0].dateStarted ? dayjs(opts[0].dateStarted) : null,
        opts[0].dateEnded ? dayjs(opts[0].dateEnded) : null
      ]
    });
  };

  // 切换 phase
  const handlePhaseChange = idx => {
    const p = phaseOptions.find(o => o.value === idx);
    if (!p) return;
    form.setFieldsValue({
      daterange: [
        p.dateStarted ? dayjs(p.dateStarted) : null,
        p.dateEnded ? dayjs(p.dateEnded) : null
      ]
    });
  };

  // 切换分区
  const handleCategoryChange = val => {
    setCategory(val);
    form.setFieldsValue({ metric: [] });
  };

  // All Search 按钮事件，自动填充 all（你可自定义逻辑）
  const handleAllSearch = () => {
    const values = form.getFieldsValue();
    // 设为全量搜索，比如 metric = ['all'] 或类似逻辑
    onAllSearch && onAllSearch({
      ...values,
      metric: ['all']
    });
  };

  return loading ? (
    <Spin />
  ) : (
    <Form
      form={form}
      layout="inline"
      onFinish={onSearch}
      initialValues={{ category: 'business', metric: [] }}
    >
      <Form.Item name="experimentName" rules={[{ required: true }]}>
        <ExperimentSelector onChange={handleExpChange} />
      </Form.Item>
      <Form.Item name="phase" rules={[{ required: true }]}>
        <PhaseSelector options={phaseOptions} onChange={handlePhaseChange} />
      </Form.Item>
      <Form.Item name="daterange" rules={[{ required: true }]}>
        <DateRangePicker />
      </Form.Item>
      <Form.Item name="category" rules={[{ required: true }]}>
        <Select
          style={{ width: 140 }}
          value={category}
          onChange={handleCategoryChange}
        >
          <Option value="business">Business</Option>
          <Option value="retention">Retention</Option>
          <Option value="engagement">Engagement</Option>
          <Option value="chat">Chat</Option>
        </Select>
      </Form.Item>
      <Form.Item name="metric" rules={[{ required: true }]}>
        <MetricSelector
          category={category}
          value={form.getFieldValue('metric')}
          metricOptionsMap={metricOptionsMap}
          onChange={vals => form.setFieldsValue({ metric: vals })}
        />
      </Form.Item>
      <Form.Item>
        <Button type="primary" htmlType="submit" style={{ marginRight: 8 }}>
          Search
        </Button>
        <Button
          type="primary"
          onClick={handleAllSearch}
        >
          All Search
        </Button>
      </Form.Item>
    </Form>
  );
}
