// src/components/CohortMetricSelector.jsx
import React from 'react';
import { Select } from 'antd';

const options = [
  { label: 'ltv', value: 'ltv' },
  { label: 'lt', value: 'lt' },
  { label: 'retention', value: 'retention' },
];

export default function CohortMetricSelector({ value, onChange, style }) {
  return (
    <Select
      value={value === undefined || value === null ? undefined : value}
      onChange={onChange}
      options={options}
      style={{
        width: 120,
        fontWeight: 500,
        ...style,   // 允许父组件自定义额外样式
      }}
      placeholder="Cohort"
      allowClear={false}   // 不允许点X清空
    />
  );
}
