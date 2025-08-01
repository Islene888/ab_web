// src/components/SearchTable/MetricSelector.jsx
import React from 'react';
import { Select } from 'antd';
const { Option } = Select;

export default function MetricSelector({ category, value, onChange, metricOptionsMap, disabled, style }) {
  const opts = metricOptionsMap[category] || [];
  return (
    <Select
      placeholder="Metric"
      value={value}
      onChange={onChange}
      style={style}
      disabled={disabled}
      allowClear
    >
      <Option value="all">All Metrics</Option>
      {opts.map(o => (
        <Option key={o.value} value={o.value}>{o.label}</Option>
      ))}
    </Select>
  );
}
