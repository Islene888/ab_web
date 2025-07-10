import numpy as np
from flask import Flask, request, jsonify
from sqlalchemy import create_engine, text
import os, urllib.parse


app = Flask(__name__)

def get_db_connection():
    password = urllib.parse.quote_plus(os.environ.get('DB_PASSWORD', 'flowgpt@2024.com'))
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    return create_engine(DATABASE_URL)

def bayesian_summary(samples):
    samples = np.array(samples)
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

def generic_bayesian_api(fetch_func, value_field, revenue_field, order_field, variation_field=None, date_field=None):
    experiment_name = request.args.get('experiment_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not experiment_name or not start_date or not end_date:
        return jsonify({"error": "请提供 experiment_name, start_date, end_date 参数"}), 400
    engine = get_db_connection()
    rows = fetch_func(experiment_name, start_date, end_date, engine)
    print(f"实验 {experiment_name} 查询到 {len(rows)} 条记录")
    for row in rows:
        print(row)
    from collections import defaultdict
    group_dict = defaultdict(list)
    group_revenue = defaultdict(float)
    group_order = defaultdict(int)
    for row in rows:
        variation_id = row[variation_field] if variation_field is not None else row[0]
        value = row[value_field]
        revenue = row[revenue_field]
        order = row[order_field]
        if value is not None:
            group_dict[variation_id].append(float(value))
            group_revenue[variation_id] += float(revenue)
            group_order[variation_id] += int(order)
    result = {
        "groups": [],
        "distribution": {str(k): v for k, v in group_dict.items()}
    }
    for group, value_list in group_dict.items():
        summary = bayesian_summary(value_list)
        summary["group"] = group
        summary["total_revenue"] = group_revenue[group]
        summary["total_order"] = group_order[group]
        result["groups"].append(summary)
    return jsonify(result)

def generic_trend_api(fetch_func, value_field, revenue_field, order_field, variation_field=None, date_field=None):
    experiment_name = request.args.get('experiment_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not experiment_name or not start_date or not end_date:
        return "参数缺失", 400
    engine = get_db_connection()
    rows = fetch_func(experiment_name, start_date, end_date, engine)
    from collections import defaultdict
    date_set = set()
    group_value = defaultdict(dict)
    group_revenue = defaultdict(dict)
    group_order = defaultdict(dict)
    for row in rows:
        variation_id = row[variation_field] if variation_field is not None else row[0]
        event_date = row[date_field] if date_field is not None else row[1]
        revenue = row[revenue_field]
        order = row[order_field]
        value = row[value_field]
        date_str = str(event_date)
        date_set.add(date_str)
        group_value[variation_id][date_str] = float(value) if value is not None else None
        group_revenue[variation_id][date_str] = float(revenue)
        group_order[variation_id][date_str] = int(order)
    dates = sorted(list(date_set))
    series = []
    for group in group_value:
        data = [group_value[group].get(date, None) for date in dates]
        revenue = [group_revenue[group].get(date, None) for date in dates]
        order = [group_order[group].get(date, None) for date in dates]
        series.append({
            "variation": group,
            "data": data,
            "revenue": revenue,
            "order": order
        })
    return jsonify({"dates": dates, "series": series})

# 主函数自动注册所有指标接口
# 只保留注册函数，不再包含指标列表和主入口
def make_bayesian_api(cfg):
    def api_func():
        return generic_bayesian_api(cfg["fetch_func"], cfg["value_field"], cfg["revenue_field"], cfg["order_field"], cfg.get("variation_field"), cfg.get("date_field"))
    return api_func

def make_trend_api(cfg):
    def api_func():
        return generic_trend_api(cfg["fetch_func"], cfg["value_field"], cfg["revenue_field"], cfg["order_field"], cfg.get("variation_field"), cfg.get("date_field"))
    return api_func

def register_indicator_routes(app, config):
    for name, cfg in config.items():
        app.add_url_rule(
            f'/api/{name}_bayesian',
            f'{name}_bayesian_api',
            make_bayesian_api(cfg),
            methods=['GET']
        )
        app.add_url_rule(
            f'/api/{name}_trend',
            f'{name}_trend_api',
            make_trend_api(cfg),
            methods=['GET']
        )
