# backend/service/cohort.py

from flask import Blueprint, request, jsonify
from ..service.config import INDICATOR_CONFIG
from ..utils.cache_utils import get_abtest_cache, set_abtest_cache
from ..utils.engine_utils import get_local_cache_engine, get_db_connection

# bp_cohort = Blueprint('cohort', __name__)
bp_cohort = Blueprint('cohort', __name__, url_prefix='/api/cohort')

# ============= 通用趋势（累计指标）接口 =============
def generic_cohort_trend_api(fetch_func, value_field, revenue_field, order_field, variation_field=None, date_field=None):
    experiment_name = request.args.get('experiment_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    metric = request.args.get('metric', '')
    category = request.args.get('category', '')
    mode = 'trend'
    if not experiment_name or not start_date or not end_date:
        return jsonify({"error": "缺少参数"}), 400

    cache_engine = get_local_cache_engine()
    cache = get_abtest_cache(
        cache_engine, query_type=mode,
        experiment_name=experiment_name,
        metric=metric or '',
        category=category or '',
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
        value = row[value_field]
        revenue = row[revenue_field] if revenue_field is not None else None
        order = row[order_field] if order_field is not None else None
        date_str = str(event_date)
        date_set.add(date_str)
        group_value[variation_id][date_str] = float(value) if value is not None else None
        group_revenue[variation_id][date_str] = float(revenue) if revenue is not None else None
        group_order[variation_id][date_str] = int(order) if order is not None else None
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
        category=category or '',
        start_date=start_date,
        end_date=end_date,
        result_json=result
    )
    return jsonify(result)

# ============= 通用热力图（单日 cohort 指标）接口 =============
def generic_cohort_heatmap_api(fetch_func, value_field, revenue_field, order_field, variation_field=None, date_field=None, extra_field=None):
    experiment_name = request.args.get('experiment_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    metric = request.args.get('metric', '')
    category = request.args.get('category', '')
    mode = 'heatmap'
    if not experiment_name or not start_date or not end_date:
        return jsonify({"error": "缺少参数"}), 400

    cache_engine = get_local_cache_engine()
    cache = get_abtest_cache(
        cache_engine, query_type=mode,
        experiment_name=experiment_name,
        metric=metric or '',
        category=category or '',
        start_date=start_date,
        end_date=end_date
    )
    if cache:
        return jsonify(cache)

    engine = get_db_connection()
    rows = fetch_func(experiment_name, start_date, end_date, engine)
    from collections import defaultdict
    data_map = defaultdict(lambda: defaultdict(dict))
    for row in rows:
        variation_id = row[variation_field] if variation_field is not None else row[0]
        event_date = row[date_field] if date_field is not None else row[1]
        value = row[value_field]
        # extra_field 指 cohort_day 这类字段
        if extra_field:
            cohort_val = row[extra_field]
            data_map[variation_id][str(event_date)][cohort_val] = value
        else:
            data_map[variation_id][str(event_date)] = value
    result = {"heatmap": data_map}
    set_abtest_cache(
        cache_engine, query_type=mode,
        experiment_name=experiment_name,
        metric=metric or '',
        category=category or '',
        start_date=start_date,
        end_date=end_date,
        result_json=result
    )
    return jsonify(result)

def make_cohort_trend_api(cfg):
    def api_func():
        return generic_cohort_trend_api(
            cfg["fetch_func"], cfg["value_field"], cfg["revenue_field"], cfg["order_field"],
            cfg.get("variation_field"), cfg.get("date_field"))
    return api_func

def make_cohort_heatmap_api(cfg, extra_field=None):
    def api_func():
        return generic_cohort_heatmap_api(
            cfg["fetch_func"], cfg["value_field"], cfg["revenue_field"], cfg["order_field"],
            cfg.get("variation_field"), cfg.get("date_field"), extra_field=extra_field)
    return api_func

def register_cohort_routes(bp, config):
    # --- 累计型趋势图 ---
    bp.add_url_rule(
        '/cumulative_retention_trend',
        'cumulative_retention_cohort_trend_api',
        make_cohort_trend_api(config["cumulative_retention"]),
        methods=['GET']
    )
    bp.add_url_rule(
        '/cumulative_ltv_trend',
        'cumulative_ltv_cohort_trend_api',
        make_cohort_trend_api(config["cumulative_ltv"]),
        methods=['GET']
    )
    bp.add_url_rule(
        '/cumulative_lt_trend',
        'cumulative_lt_cohort_trend_api',
        make_cohort_trend_api(config["cumulative_lt"]),
        methods=['GET']
    )
    # --- 热力图路由 ---
    bp.add_url_rule(
        '/active_retention_d1_heatmap',
        'active_retention_d1_heatmap_api',
        make_cohort_heatmap_api(config["all_retention_d1"]),
        methods=['GET']
    )
    bp.add_url_rule(
        '/arpu_heatmap',
        'arpu_heatmap_api',
        make_cohort_heatmap_api(config["cohort_arpu"]),
        methods=['GET']
    )
    bp.add_url_rule(
        '/time_spend_heatmap',
        'time_spend_heatmap_api',
        make_cohort_heatmap_api(config["avg_time_spent"]),
        methods=['GET']
    )


# 注册蓝图
register_cohort_routes(bp_cohort, INDICATOR_CONFIG)
