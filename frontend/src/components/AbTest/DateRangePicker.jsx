// src/components/AbTest/DateRangePicker.jsx

import React from 'react';
import { DatePicker } from 'antd';
const { RangePicker } = DatePicker;

/**
 * Props:
 *   value: [moment, moment]
 *   onChange([moment, moment])
 */
export default function DateRangePicker({ value, onChange }) {
  return (
    <RangePicker
      value={value}
      onChange={onChange}
      allowEmpty={[false, true]}
    />
  );
}
