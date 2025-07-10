import React, { useState } from 'react';
import { Input, DatePicker, Button, Form, Select } from 'antd';
import 'antd/dist/reset.css';

const { Option } = Select;

// Category -> Metric 映射
const metricOptionsMap = {
  business: [
    { value: 'aov', label: 'AOV' },
    { value: 'arpu', label: 'ARPU' },
    { value: 'arppu', label: 'ARPPU' },
    { value: 'subscribe_rate', label: 'Subscribe Rate' },
    { value: 'payment_rate_all', label: 'Payment Rate (All)' },
    { value: 'payment_rate_new', label: 'Payment Rate (New Users)' },
    { value: 'ltv', label: 'LTV' },
    { value: 'cancel_sub', label: 'Cancel Subscribe Rate' },
    { value: 'first_new_sub', label: 'First New User Subscribe Rate' },
    { value: 'recharge_rate', label: 'Recharge Rate' }
  ],
  engagement: [
    { value: 'continue', label: 'Continue' },
    { value: 'conversation_reset', label: 'Conversation Reset' },
    { value: 'edit', label: 'Edit' },
    { value: 'follow', label: 'Follow' },
    { value: 'message', label: 'Message' },
    { value: 'new_conversation', label: 'New Conversation' },
    { value: 'regen', label: 'Regen' }
  ],
  retention: [
    { value: 'all_retention', label: 'All Retention' },
    { value: 'new_retention', label: 'New Retention' }
  ],
  recharge: [
    { value: 'recharge_rate', label: 'Recharge Rate' }
  ],
  chat: [
    { value: 'click_rate', label: 'Click Rate' },
    { value: 'explore_start_chat_rate', label: 'Explore Start Chat Rate' },
    { value: 'avg_chat_rounds', label: 'Avg Chat Rounds per User' },
    { value: 'avg_start_chat_bots', label: 'Avg Start Chat Bots per User' },
    { value: 'avg_click_bots', label: 'Avg Click Bots per User' },
    { value: 'avg_time_spent', label: 'Avg Time Spent per User' },
    { value: 'explore_click_rate', label: 'Explore Click Rate' },
    { value: 'explore_avg_chat_rounds', label: 'Explore Avg Chat Rounds per User' }
  ]
};

function App() {
  const [params, setParams] = useState(null);
  const [form] = Form.useForm();
  const [category, setCategory] = useState('business');

  const onFinish = (values) => {
    setParams({
      experimentName: values.experimentName,
      category: values.category,
      metric: values.metric,
      startDate: values.daterange[0].format('YYYY-MM-DD'),
      endDate: values.daterange[1].format('YYYY-MM-DD'),
    });
  };

  // 动态渲染 Metric 选项
  const metricOptions = metricOptionsMap[category] || [];
  // 添加"所有指标"选项
  const allMetricsOption = { value: 'all', label: 'All Metrics' };
  const metricOptionsWithAll = [allMetricsOption, ...metricOptions];

  return (
    <div style={{ width: '95%', maxWidth: 2500, margin: '40px auto' }}>
      <div style={{ display: 'flex', alignItems: 'center', marginBottom: 32 }}>
        <Form
          form={form}
          layout="inline"
          onFinish={onFinish}
          style={{ marginBottom: 32, justifyContent: 'center', display: 'flex' }}
          initialValues={{ metric: [], category: 'business' }}
        >
          <Form.Item
            name="experimentName"
            label="Experiment Name"
            rules={[{ required: true, message: 'Please input experiment name' }]}
          >
            <Input placeholder="Please input experiment name" style={{ width: 220 }} />
          </Form.Item>
          <Form.Item
            name="daterange"
            label="Date Range"
            rules={[{ required: true, message: 'Please select experiment period' }]}
          >
            <DatePicker.RangePicker style={{ width: 260 }} />
          </Form.Item>
          <Form.Item
            name="category"
            label="Category"
            rules={[{ required: true, message: 'Please select a category' }]}
          >
            <Select
              style={{ width: 180 }}
              onChange={val => {
                setCategory(val);
                form.setFieldsValue({ metric: [] });
              }}
            >
              <Option value="retention">Retention</Option>
              <Option value="business">Business</Option>
              <Option value="engagement">Engagement</Option>
              <Option value="chat">Chat Behavior</Option>
            </Select>
          </Form.Item>
          <Form.Item
            name="metric"
            label="Metric"
            rules={[{ required: true, message: 'Please select metrics' }]}
          >
            <Select
              style={{ width: 220 }}
              mode="multiple"
              allowClear
              placeholder="Select metrics"
              onChange={selected => {
                // 如果选了 all，只保留 all
                if (selected.includes('all')) {
                  form.setFieldsValue({ metric: ['all'] });
                } else {
                  // 如果 all 没选，正常多选
                  form.setFieldsValue({ metric: selected.filter(v => v !== 'all') });
                }
              }}
            >
              {metricOptionsWithAll.map(opt => (
                <Option key={opt.value} value={opt.value}>{opt.label}</Option>
              ))}
            </Select>
          </Form.Item>
          <Form.Item>
            <Button type="primary" htmlType="submit">
              Search
            </Button>
          </Form.Item>
        </Form>
      </div>
      {params && (
        <AbTestResult
          experimentName={params.experimentName}
          metric={params.metric}
          startDate={params.startDate}
          endDate={params.endDate}
        />
      )}
    </div>
  );
}

export default App;
