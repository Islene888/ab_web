import numpy as np
from flask import Flask, request, jsonify
from sqlalchemy import create_engine, text
import os
import urllib.parse

from ..utils.cache_utils import get_abtest_cache, set_abtest_cache
from ..utils.engine_utils import get_local_cache_engine, get_db_connection

app = Flask(__name__)


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
    metric = request.args.get('metric', '')
    category = request.args.get('category', '')
    mode = 'bayesian'
    if not experiment_name or not start_date or not end_date:
        return jsonify({"error": "请提供 experiment_name, start_date, end_date 参数"}), 400

    # 本地缓存引擎
    cache_engine = get_local_cache_engine()
    # 查询本地缓存
    cache = get_abtest_cache(
        cache_engine, query_type=mode,
        experiment_name=experiment_name,
        metric=metric or '',
        category=category or '',
        start_date=start_date,
        end_date=end_date
    )
    if cache:
        print(f"[CACHE-HIT] [{mode}] [{metric}] 命中缓存，直接返回")
        return jsonify(cache)
    print(f"[CACHE-MISS] [{mode}] [{metric}] 未命中缓存，开始实时计算...")

    # 查主数据仓库
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
    # 存本地缓存
    set_abtest_cache(
        cache_engine, query_type=mode,
        experiment_name=experiment_name,
        metric=metric or '',
        category=category or '',
        start_date=start_date,
        end_date=end_date,
        result_json=result
    )
    print(f"[CACHE-SET] [{mode}] [{metric}] 实时计算完成，已写入缓存")
    return jsonify(result)

def generic_trend_api(fetch_func, value_field, revenue_field, order_field, variation_field=None, date_field=None):
    from backend.service.config import INDICATOR_CONFIG  # 自动查 category
    experiment_name = request.args.get('experiment_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    metric = request.args.get('metric', '')
    category = request.args.get('category', '')
    mode = 'trend'
    if not experiment_name or not start_date or not end_date:
        return "参数缺失", 400

    # ----------- 自动兜底补 category ----------
    if not category and metric:
        # config 里查当前 metric 的 category（如 business/retention/chat/...）
        category = INDICATOR_CONFIG.get(metric, {}).get("category", "")
    # ------------------------------------------

    cache_engine = get_local_cache_engine()
    cache = get_abtest_cache(
        cache_engine, query_type=mode,
        experiment_name=experiment_name,
        metric=metric or '',
        category=category or '',  # 保证总是有category
        start_date=start_date,
        end_date=end_date
    )
    if cache:
        return jsonify(cache)

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
    result = {"dates": dates, "series": series}
    set_abtest_cache(
        cache_engine, query_type=mode,
        experiment_name=experiment_name,
        metric=metric or '',
        category=category or '',   # 关键点
        start_date=start_date,
        end_date=end_date,
        result_json=result
    )
    return jsonify(result)


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
