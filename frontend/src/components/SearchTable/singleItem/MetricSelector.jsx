// src/components/SearchTable/MetricSelector.jsx

import React from 'react';
import { Select } from 'antd';
const { Option } = Select;

/**
 * Props:
 *   category: string
 *   value: array of metric values
 *   onChange(vals)
 *   metricOptionsMap: from api/growthbook
 */
export default function MetricSelector({ category, value, onChange, metricOptionsMap }) {
  const opts = metricOptionsMap[category] || [];
  return (
    <Select
      placeholder="Metric"
      value={value}
      onChange={onChange}
      style={{ width: 200 }}
    >
      <Option value="all">All Metrics</Option>
      {opts.map(o => (
        <Option key={o.value} value={o.value}>{o.label}</Option>
      ))}
    </Select>
  );
}
