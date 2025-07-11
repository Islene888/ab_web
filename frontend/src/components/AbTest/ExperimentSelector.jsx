// src/components/AbTest/ExperimentSelector.jsx

import React, { useEffect, useState } from 'react';
import { Select, Spin } from 'antd';
const { Option } = Select;

/**
 * Props:
 *   value: selected experiment name
 *   onChange(expName)
 */
export default function ExperimentSelector({ value, onChange }) {
  const [list, setList] = useState([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    setLoading(true);
    fetch('/api/experiments')
      .then(r => r.json())
      .then(data => setList(data))
      .finally(() => setLoading(false));
  }, []);

  return loading
    ? <Spin />
    : (
      <Select
        showSearch
        placeholder="Select experiment"
        value={value}
        onChange={onChange}
        style={{ width: 220 }}
        optionFilterProp="children"
      >
        {list.map(exp => (
          <Option key={exp.experiment_name} value={exp.experiment_name}>
            {exp.experiment_name}
          </Option>
        ))}
      </Select>
    );
}
