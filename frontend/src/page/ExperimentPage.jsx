// src/page/ExperimentPage.jsx
import React, { useEffect, useState } from "react";
import { Table } from "antd";
import { useNavigate } from "react-router-dom";

const columns = [
  { title: "实验名称", dataIndex: "experiment_name", key: "experiment_name" },
  { title: "标签", dataIndex: "tags", key: "tags" },
  { title: "对照组", dataIndex: "control_group_key", key: "control_group_key" },
  { title: "变体数", dataIndex: "number_of_variations", key: "number_of_variations" },
  { title: "阶段", dataIndex: "phase_name", key: "phase_name" },   // 新增
  { title: "开始时间", dataIndex: "phase_start_time", key: "phase_start_time" },
  { title: "结束时间", dataIndex: "phase_end_time", key: "phase_end_time" },
];


export default function ExperimentPage() {
  const [data, setData] = useState([]);
  const navigate = useNavigate();

  useEffect(() => {
    fetch("/api/experiments")
      .then((res) => res.json())
      .then((d) => setData(d));
  }, []);

  return (
    <div style={{ padding: 32, background: "#20213a", minHeight: "100vh" }}>
      <h1 style={{ textAlign: "center", color: "#fff", marginBottom: 24 }}>实验信息总览</h1>
      <Table
        rowKey="experiment_name"
        columns={columns}
        dataSource={data}
        pagination={{ pageSize: 10 }}
        onRow={record => ({
          onClick: () => navigate(`/abtest?experiment=${encodeURIComponent(record.experiment_name)}`),
          style: { cursor: "pointer" }
        })}
      />
    </div>
  );
}
