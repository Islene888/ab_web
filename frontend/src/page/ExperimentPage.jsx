// src/pages/ExperimentPage.jsx

import React, { useEffect, useState } from "react";
import { Table, Tag, Empty } from "antd";
import { useNavigate } from "react-router-dom";
import dayjs from "dayjs";
import isSameOrAfter from "dayjs/plugin/isSameOrAfter";
dayjs.extend(isSameOrAfter);

// Tag styles: white text on dark gray
const blueTag = {
  background: "#333a56",
  color: "#fff",
  border: 0,
  fontWeight: 500,
  fontSize: 14,
  userSelect: "none",
};
const grayTag = {
  background: "#2a2b3d",
  color: "#ddd",
  border: 0,
  fontWeight: 400,
  fontSize: 14,
  userSelect: "none",
};

const columns = [
  {
    title: (
      <span
        style={{
          fontWeight: 700,
          fontSize: 17,
          color: "#fff",
          textAlign: "center",
          display: "block",
          userSelect: "none",
        }}
      >
        Experiment
      </span>
    ),
    dataIndex: "experiment_name",
    key: "experiment_name",
    ellipsis: true,
    render: (text) => <span style={{ color: "#fff" }}>{text}</span>,
  },
  {
    title: (
      <span
        style={{
          fontWeight: 700,
          fontSize: 17,
          color: "#fff",
          textAlign: "center",
          display: "block",
          userSelect: "none",
        }}
      >
        Phase
      </span>
    ),
    dataIndex: "phase_name",
    key: "phase_name",
    ellipsis: true,
    width: 110,
    render: (text) => <span style={{ color: "#fff" }}>{text || ""}</span>,
  },
  {
    title: (
      <span
        style={{
          fontWeight: 700,
          fontSize: 17,
          color: "#fff",
          textAlign: "center",
          display: "block",
          userSelect: "none",
        }}
      >
        Start Time
      </span>
    ),
    dataIndex: "phase_start_time",
    key: "phase_start_time",
    ellipsis: true,
    align: "center",
    render: (text) => (
      <span style={{ color: "#fff", display: "block", textAlign: "center" }}>
        {text}
      </span>
    ),
  },
  {
    title: (
      <span
        style={{
          fontWeight: 700,
          fontSize: 17,
          color: "#fff",
          textAlign: "center",
          display: "block",
          userSelect: "none",
        }}
      >
        End Time
      </span>
    ),
    dataIndex: "phase_end_time",
    key: "phase_end_time",
    ellipsis: true,
    align: "center",
    render: (text) => (
      <span style={{ color: "#fff", display: "block", textAlign: "center" }}>
        {text}
      </span>
    ),
  },
  {
    title: (
      <span
        style={{
          fontWeight: 700,
          fontSize: 17,
          color: "#fff",
          textAlign: "center",
          display: "block",
          userSelect: "none",
        }}
      >
        Variations
      </span>
    ),
    dataIndex: "number_of_variations",
    key: "number_of_variations",
    align: "center",
    width: 110,
    render: (num) => (
      <span style={{ color: "#fff", fontWeight: 700 }}>{num}</span>
    ),
  },
  {
    title: (
      <span
        style={{
          fontWeight: 700,
          fontSize: 17,
          color: "#fff",
          textAlign: "center",
          display: "block",
          userSelect: "none",
        }}
      >
        Tags
      </span>
    ),
    dataIndex: "tags",
    key: "tags",
    ellipsis: true,
    align: "center",
    render: (tags) => {
      if (!tags) return <Tag style={grayTag}>None</Tag>;
      return tags.split(",").map((tag) => (
        <Tag style={blueTag} key={tag.trim()}>
          {tag.trim()}
        </Tag>
      ));
    },
  },
];

export default function ExperimentPage() {
  const [data, setData] = useState([]);
  const navigate = useNavigate();

  useEffect(() => {
    fetch("/api/experiments")
      .then((res) => res.json())
      .then((expData) => {
        const flatData = [];
        expData.forEach((exp) => {
          (exp.phases || []).forEach((phase, idx) => {
            flatData.push({
              experiment_name: exp.experiment_name,
              phase_name: phase.name || "",
              phase_start_time: phase.dateStarted
                ? dayjs(phase.dateStarted).format("YYYY-MM-DD HH:mm")
                : "-",
              phase_end_time: phase.dateEnded
                ? dayjs(phase.dateEnded).format("YYYY-MM-DD HH:mm")
                : "now",
              number_of_variations: exp.number_of_variations,
              tags:
                typeof exp.tags === "string"
                  ? exp.tags
                  : Array.isArray(exp.tags)
                  ? exp.tags.join(", ")
                  : "",
              _phaseIdx: idx,
            });
          });
        });

        // Only keep experiments whose start time is within the last 3 months
        const threeMonthsAgo = dayjs().subtract(3, "month");
        const filtered = flatData.filter((row) =>
          row.phase_start_time !== "-" &&
          dayjs(row.phase_start_time, "YYYY-MM-DD HH:mm").isSameOrAfter(
            threeMonthsAgo,
            "day"
          )
        );

        // Sort by start time descending
        filtered.sort(
          (a, b) =>
            dayjs(b.phase_start_time).valueOf() -
            dayjs(a.phase_start_time).valueOf()
        );

        setData(filtered);
      });
  }, []);

  const rowClassName = () => "dark-table-row";

  return (
    <div
      style={{
        minHeight: "100vh",
        background: "#18192a",
        padding: "48px 0",
      }}
    >
      <div
        style={{
          maxWidth: 1100,
          margin: "0 auto",
          background: "#1c1d2b",
          borderRadius: 24,
          padding: "32px 32px 16px 32px",
          border: "1.5px solid #2b2e43",
        }}
      >
        <h1
          style={{
            textAlign: "center",
            color: "#fff",
            marginBottom: 32,
            fontWeight: 900,
            fontSize: 36,
            letterSpacing: 3,
            userSelect: "none",
          }}
        >
          Experiment Phase Overview
        </h1>
        <Table
          rowKey={(row) =>
            `${row.experiment_name}_${row.phase_name || row._phaseIdx}_${row.phase_start_time}`
          }
          columns={columns}
          dataSource={data}
          locale={{
            emptyText: (
              <Empty
                description="No experiments in the last 3 months"
                image={Empty.PRESENTED_IMAGE_SIMPLE}
                style={{ color: "#fff" }}
              />
            ),
          }}
          pagination={{ pageSize: 10, showSizeChanger: true }}
          rowClassName={rowClassName}
          onRow={(record) => ({
            onClick: () =>
              navigate(
                `/abtest?experiment=${encodeURIComponent(
                  record.experiment_name
                )}&phase=${record._phaseIdx}`
              ),
            style: { cursor: "pointer", transition: "background 0.18s" },
          })}
          style={{
            borderRadius: 18,
            overflow: "hidden",
            background: "#191a27",
          }}
        />
      </div>
      <style>{`
        .dark-table-row td {
          background: #18192a !important;
          color: #fff !important;
          border-bottom: 1px solid #23243a;
        }
        .dark-table-row:hover td {
          background: #26304a !important;
        }
        .ant-table-thead > tr > th {
          background: #1c1d2b !important;
          color: #fff !important;
          font-weight: 700;
          font-size: 17px;
          border-bottom: 2px solid #23243a;
          user-select: none;
          text-align: center;    /* 居中，建议全局就加 */
          white-space: nowrap;   /* 强制表头不折行 */
        }

      `}</style>
    </div>
  );
}
