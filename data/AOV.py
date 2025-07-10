import logging
import os
import urllib.parse
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import numpy as np
from flask import Flask, request, jsonify
import warnings
import sys
warnings.filterwarnings("ignore", category=FutureWarning)
load_dotenv()

app = Flask(__name__)

def get_db_connection():
    password = urllib.parse.quote_plus(os.environ.get('DB_PASSWORD', 'flowgpt@2024.com'))
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    logging.info("✅ 数据库连接已建立。")
    return engine

def fetch_group_aov_samples(experiment_name, start_date, end_date, engine):
    query = f'''
    SELECT
      variation_id,
      event_date,
      SUM(revenue) AS total_revenue,
      COUNT(*) AS total_order_cnt,
      ROUND(SUM(revenue) * 1.0 / NULLIF(COUNT(*), 0), 2) AS aov
    FROM (
      SELECT
        eu.variation_id,
        o.event_date,
        o.revenue
      FROM flow_event_info.tbl_app_event_currency_purchase o
      JOIN (
        SELECT
          user_id,
          CAST(variation_id AS CHAR) AS variation_id,
          MIN(timestamp_assigned) AS timestamp_assigned
        FROM flow_wide_info.tbl_wide_experiment_assignment_hi
        WHERE experiment_id = '{experiment_name}'
          AND timestamp_assigned BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'
        GROUP BY user_id, variation_id
      ) eu ON o.user_id = eu.user_id
      WHERE o.event_date BETWEEN '{start_date}' AND '{end_date}'
        AND o.event_date >= DATE(eu.timestamp_assigned)
      UNION ALL
      SELECT
        eu.variation_id,
        o.event_date,
        o.revenue
      FROM flow_event_info.tbl_app_event_subscribe o
      JOIN (
        SELECT
          user_id,
          CAST(variation_id AS CHAR) AS variation_id,
          MIN(timestamp_assigned) AS timestamp_assigned
        FROM flow_wide_info.tbl_wide_experiment_assignment_hi
        WHERE experiment_id = '{experiment_name}'
          AND timestamp_assigned BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'
        GROUP BY user_id, variation_id
      ) eu ON o.user_id = eu.user_id
      WHERE o.event_date BETWEEN '{start_date}' AND '{end_date}'
        AND o.event_date >= DATE(eu.timestamp_assigned)
    ) t
    GROUP BY variation_id, event_date
    ORDER BY variation_id, event_date;
    '''
    with engine.connect() as conn:
        df = conn.execute(text(query)).fetchall()
    print(f"实验 {experiment_name} 查询到 {len(df)} 条记录")
    print(df)
    return df

def bayesian_summary(aov_samples):
    samples = np.array(aov_samples)
    mean = np.mean(samples)
    std = np.std(samples, ddof=1)
    n = len(samples)
    posterior_samples = np.random.normal(mean, std / np.sqrt(n), 1000)
    ci_lower, ci_upper = np.percentile(posterior_samples, [2.5, 97.5])
    return {
        "mean": float(mean),
        "std": float(std),
        "n": int(n),
        "posterior_samples": posterior_samples.tolist(),
        "credible_interval": [float(ci_lower), float(ci_upper)]
    }

@app.route('/api/aov_bayesian', methods=['GET'])
def aov_bayesian_api():
    experiment_name = request.args.get('experiment_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    metric = request.args.get('metric')
    if not experiment_name or not start_date or not end_date:
        return jsonify({"error": "请提供 experiment_name, start_date, end_date 参数"}), 400
    engine = get_db_connection()
    rows = fetch_group_aov_samples(experiment_name, start_date, end_date, engine)
    group_dict = {}
    group_revenue = {}
    group_order = {}
    for row in rows:
        variation_id = row[0]
        aov = row[4]
        revenue = row[2]
        order_cnt = row[3]
        if aov is not None:
            group_dict.setdefault(variation_id, []).append(float(aov))
            group_revenue[variation_id] = group_revenue.get(variation_id, 0) + float(revenue)
            group_order[variation_id] = group_order.get(variation_id, 0) + int(order_cnt)
    result = {
        "groups": [],
        "distribution": group_dict
    }
    for group, aov_list in group_dict.items():
        summary = bayesian_summary(aov_list)
        summary["group"] = group
        summary["total_revenue"] = group_revenue.get(group, 0)
        summary["total_order"] = group_order.get(group, 0)
        result["groups"].append(summary)
    return jsonify(result)

@app.route('/api/aov_trend', methods=['GET'])
def aov_trend_api():
    experiment_name = request.args.get('experiment_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not experiment_name or not start_date or not end_date:
        return "参数缺失", 400
    engine = get_db_connection()
    rows = fetch_group_aov_samples(experiment_name, start_date, end_date, engine)
    from collections import defaultdict
    date_set = set()
    group_aov = defaultdict(dict)
    group_revenue = defaultdict(dict)
    group_order = defaultdict(dict)
    for row in rows:
        variation_id, event_date, revenue, order_cnt, aov = row
        date_str = str(event_date)
        date_set.add(date_str)
        group_aov[variation_id][date_str] = float(aov) if aov is not None else None
        group_revenue[variation_id][date_str] = float(revenue)
        group_order[variation_id][date_str] = int(order_cnt)
    dates = sorted(list(date_set))
    series = []
    for group in group_aov:
        data = [group_aov[group].get(date, None) for date in dates]
        revenue = [group_revenue[group].get(date, None) for date in dates]
        order = [group_order[group].get(date, None) for date in dates]
        series.append({
            "variation": group,
            "data": data,
            "revenue": revenue,
            "order": order
        })
    return jsonify({"dates": dates, "series": series})

# if __name__ == "__main__":
    # app.run(host="0.0.0.0", port=5050, debug=True)
