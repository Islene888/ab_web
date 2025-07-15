// src/components/SearchTable/SearchForm.jsx

import React, { useEffect, useState } from 'react';
import { Form, Button, Spin, Select } from 'antd';
import dayjs from 'dayjs'; // 确保 dayjs 已导入
import ExperimentSelector from './singleItem/ExperimentSelector';
import PhaseSelector from './singleItem/PhaseSelector';
import DateRangePicker from './singleItem/DateRangePicker';
import MetricSelector from './singleItem/MetricSelector';
import { metricOptionsMap } from '../../config/metricOptionsMap';
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
            // ✅ 修改点 1: 增加对字符串 'now' 的判断
            (ph.dateEnded && ph.dateEnded !== 'now') ? dayjs(ph.dateEnded) : dayjs()
          ],
          category: 'business',
          metric: []
        });
        setCategory('business');
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
    const firstPhase = opts[0]; // 取第一个阶段作为默认值
    form.setFieldsValue({
      phase: firstPhase.value,
      daterange: [
        firstPhase.dateStarted ? dayjs(firstPhase.dateStarted) : null,
        // ✅ 修改点 2: 增加对字符串 'now' 的判断
        (firstPhase.dateEnded && firstPhase.dateEnded !== 'now') ? dayjs(firstPhase.dateEnded) : dayjs()
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
        // ✅ 修改点 3: 增加对字符串 'now' 的判断
        (p.dateEnded && p.dateEnded !== 'now') ? dayjs(p.dateEnded) : dayjs()
      ]
    });
  };

  // 切换分区
  const handleCategoryChange = val => {
    setCategory(val);
    form.setFieldsValue({ metric: [] });
  };

  // All Search 按钮事件
  const handleAllSearch = () => {
    const values = form.getFieldsValue();
    onAllSearch && onAllSearch({
      ...values,
      metric: ['all']
    });
  };

  if (loading) {
    return <Spin />;
  }

  return (
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
          metricOptionsMap={metricOptionsMap}
          value={form.getFieldValue('metric')}
          onChange={vals => form.setFieldsValue({ metric: vals })}
        />
      </Form.Item>

      <Form.Item>
        <Button type="primary" htmlType="submit" style={{ marginRight: 8 }}>
          Search
        </Button>
        <Button type="primary" onClick={handleAllSearch}>
          All Search
        </Button>
      </Form.Item>
    </Form>
  );
}