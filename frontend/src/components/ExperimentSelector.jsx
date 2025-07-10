// src/components/ExperimentSelector.jsx
import React, { useEffect, useState } from "react";
import { Select, Spin } from "antd";
import dayjs from "dayjs";

const { Option } = Select;

/**
 * 实验选择组件
 * @param {function} onChange - (experimentName, phaseInfo) => void
 */
function ExperimentSelector({ value, onChange }) {
  const [experiments, setExperiments] = useState([]);
  const [loading, setLoading] = useState(false);
  const [phaseOptions, setPhaseOptions] = useState([]);
  const [selectedExperiment, setSelectedExperiment] = useState(null);

  // 拉取实验数据
  useEffect(() => {
    setLoading(true);
    fetch("http://127.0.0.1:5055/api/experiments")
      .then((res) => res.json())
      .then((data) => setExperiments(data))
      .finally(() => setLoading(false));
  }, []);

  // 切换实验
  const handleExperimentChange = (experimentName) => {
    const exp = experiments.find(e => e.experiment_name === experimentName);
    setSelectedExperiment(exp);
    setPhaseOptions(
      exp?.phases?.map((p, idx) => ({
        label: `${p.phase_name || "阶段" + (idx + 1)} (${p.start_time} ~ ${p.end_time})`,
        value: idx,
        ...p,
      })) || []
    );
    // 默认选第一个阶段
    if (exp?.phases?.length) {
      onChange(experimentName, exp.phases[0]);
    } else {
      onChange(experimentName, null);
    }
  };

  // 切换阶段
  const handlePhaseChange = (idx) => {
    if (!selectedExperiment) return;
    const phase = selectedExperiment.phases[idx];
    onChange(selectedExperiment.experiment_name, phase);
  };

  return (
    <div style={{ display: "flex", gap: 12 }}>
      <div>
        <span style={{ color: "#fff" }}>Experiment:</span><br />
        {loading ? (
          <Spin />
        ) : (
          <Select
            showSearch
            placeholder="请选择实验"
            style={{ width: 220 }}
            onChange={handleExperimentChange}
            value={selectedExperiment?.experiment_name}
            optionFilterProp="children"
          >
            {experiments.map(exp => (
              <Option key={exp.experiment_name} value={exp.experiment_name}>
                {exp.experiment_name}
              </Option>
            ))}
          </Select>
        )}
      </div>
      <div>
        <span style={{ color: "#fff" }}>Phase:</span><br />
        <Select
          style={{ width: 260 }}
          placeholder="请选择阶段"
          value={value?.phase_name || undefined}
          disabled={!phaseOptions.length}
          onChange={handlePhaseChange}
        >
          {phaseOptions.map(phase => (
            <Option key={phase.value} value={phase.value}>
              {phase.label}
            </Option>
          ))}
        </Select>
      </div>
    </div>
  );
}

export default ExperimentSelector;
