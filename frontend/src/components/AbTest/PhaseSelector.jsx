// src/components/AbTest/PhaseSelector.jsx

import React from 'react';
import { Select } from 'antd';
const { Option } = Select;

/**
 * Props:
 *   value: phase index
 *   options: [{ value, label, dateStarted, dateEnded }]
 *   onChange(idx)
 */
export default function PhaseSelector({ value, options, onChange }) {
  return (
    <Select
      placeholder="Select phase"
      value={value}
      onChange={onChange}
      style={{ width: 260 }}
    >
      {options.map(opt => (
        <Option key={opt.value} value={opt.value}>
          {opt.label}
        </Option>
      ))}
    </Select>
  );
}
