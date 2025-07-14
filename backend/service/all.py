from flask import Blueprint, request, jsonify
from collections import defaultdict
from backend.service.config import INDICATOR_CONFIG
from functools import lru_cache

from backend.service.service import bayesian_summary
from backend.utils.cache_utils import get_abtest_cache, set_abtest_cache
from backend.utils.engine_utils import get_local_cache_engine, get_db_connection

all_bp = Blueprint("all", __name__)

@lru_cache(maxsize=1)
def get_metrics_by_category():
    category_map = defaultdict(list)
    for metric, cfg in INDICATOR_CONFIG.items():
        cat = cfg.get("category")
        if cat:
            category_map[cat].append(metric)
    return dict(category_map)

def get_metric_names(category):
    return get_metrics_by_category().get(category, [])

def getval(row, field):
    if isinstance(field, int):
        try:
            return row[field]
        except Exception:
            return None
    elif isinstance(field, str):
        if isinstance(row, dict):
            return row.get(field)
        return None
    return None

@all_bp.route('/api/all_trend', methods=['GET'])
def all_trend():
    experiment_name = request.args.get('experiment_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    category = request.args.get('category')

    if not experiment_name or not start_date or not end_date or not category:
        return jsonify({"error": "参数缺失"}), 400

    metric_names = get_metric_names(category)
    if not metric_names:
        return jsonify({"error": "未知的类别"}), 400

    # 缓存库用本地
    local_engine = get_local_cache_engine()

    # 优先查all缓存
    cache = get_abtest_cache(
        local_engine, "all_trend", experiment_name, "ALL", category, start_date, end_date
    )
    if cache:
        return jsonify(cache)

    # 只有查不到才查主库
    engine = get_db_connection()
    all_results = {}
    for metric in metric_names:
        cfg = INDICATOR_CONFIG.get(metric)
        if not cfg:
            continue
        rows = cfg["fetch_func"](experiment_name, start_date, end_date, engine)
        date_set = set()
        group_value = defaultdict(dict)
        group_revenue = defaultdict(dict)
        group_order = defaultdict(dict)
        for row in rows:
            variation_id = getval(row, cfg.get("variation_field", 0))
            event_date = getval(row, cfg.get("date_field", 1))
            revenue = getval(row, cfg["revenue_field"])
            order = getval(row, cfg["order_field"])
            value = getval(row, cfg["value_field"])
            if event_date is None or variation_id is None:
                continue
            date_str = str(event_date)
            date_set.add(date_str)
            group_value[variation_id][date_str] = float(value) if value is not None else None
            group_revenue[variation_id][date_str] = float(revenue) if revenue is not None else 0
            group_order[variation_id][date_str] = int(order) if order is not None else 0
        dates = sorted(list(date_set))
        series = []
        for group in group_value:
            data = [group_value[group].get(date, None) for date in dates]
            revenue = [group_revenue[group].get(date, 0) for date in dates]
            order = [group_order[group].get(date, 0) for date in dates]
            series.append({
                "variation": group,
                "data": data,
                "revenue": revenue,
                "order": order
            })
        all_results[metric] = {
            "dates": dates,
            "series": series
        }

    # 持久化all缓存到本地库
    set_abtest_cache(
        local_engine, "all_trend", experiment_name, "ALL", category, start_date, end_date, all_results
    )
    return jsonify(all_results)

@all_bp.route('/api/all_bayesian', methods=['GET'])
def all_bayesian():
    experiment_name = request.args.get('experiment_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    category = request.args.get('category')

    if not all([experiment_name, start_date, end_date, category]):
        return jsonify({"error": "参数缺失，请提供 experiment_name, start_date, end_date 和 category"}), 400

    # 缓存库用本地
    local_engine = get_local_cache_engine()

    # 优先查all缓存
    cache = get_abtest_cache(
        local_engine, "all_bayesian", experiment_name, "ALL", category, start_date, end_date
    )
    if cache:
        return jsonify(cache)

    metric_names = get_metric_names(category)
    if not metric_names:
        return jsonify({"error": f"未知的类别: {category}"}), 400

    # 只有查不到才查主库
    engine = get_db_connection()
    all_results = {}
    for metric in metric_names:
        cfg = INDICATOR_CONFIG.get(metric)
        if not cfg:
            continue
        rows = cfg["fetch_func"](experiment_name, start_date, end_date, engine)
        group_dict = defaultdict(list)
        group_revenue = defaultdict(float)
        group_order = defaultdict(int)
        for row in rows:
            variation_id = getval(row, cfg.get("variation_field", 0))
            value = getval(row, cfg.get("value_field"))
            revenue = getval(row, cfg.get("revenue_field"))
            order = getval(row, cfg.get("order_field"))
            if value is not None and variation_id is not None:
                group_dict[variation_id].append(float(value))
                group_revenue[variation_id] += float(revenue or 0)
                group_order[variation_id] += int(order or 0)
        metric_groups_summary = []
        for group, value_list in group_dict.items():
            if not value_list:
                continue
            summary = bayesian_summary(value_list)
            summary["group"] = group
            summary["total_revenue"] = group_revenue[group]
            summary["total_order"] = group_order[group]
            metric_groups_summary.append(summary)
        all_results[metric] = {"groups": metric_groups_summary}

    # 持久化all缓存到本地库
    set_abtest_cache(
        local_engine, "all_bayesian", experiment_name, "ALL", category, start_date, end_date, all_results
    )
    return jsonify(all_results)
