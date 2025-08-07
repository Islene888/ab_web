# backend/service/cohort.py

from flask import Blueprint, request, jsonify
from ..service.config import INDICATOR_CONFIG
from ..utils.cache_utils import get_abtest_cache, set_abtest_cache
from ..utils.engine_utils import get_local_cache_engine, get_db_connection
import pandas as pd
from collections import defaultdict

bp_cohort = Blueprint('cohort', __name__, url_prefix='/api/cohort')

# ============= 通用趋势（累计指标）接口 =============
def generic_cohort_trend_api(
    fetch_func, value_field, revenue_field, order_field,
    variation_field=None, date_field=None, metric_name=""
):
    experiment_name = request.args.get('experiment_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    category = request.args.get('category', '')
    mode = 'trend'

    if not experiment_name or not start_date or not end_date:
        return jsonify({"error": "缺少参数"}), 400

    cache_engine = get_local_cache_engine()
    cache = get_abtest_cache(
        cache_engine, query_type=mode,
        experiment_name=experiment_name,
        metric=metric_name,
        category=category or '',
        start_date=start_date,
        end_date=end_date
    )
    if cache:
        return jsonify(cache)

    engine = get_db_connection()
    rows = fetch_func(experiment_name, start_date, end_date, engine)
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
        metric=metric_name,
        category=category or '',
        start_date=start_date,
        end_date=end_date,
        result_json=result
    )
    return jsonify(result)

# ============= 通用热力图（单日 cohort 指标）接口（宽表模式） =============
# backend/service/cohort.py

# ============= 【最终修正版】通用热力图接口 =============
def generic_cohort_heatmap_api(fetch_func, value_field, revenue_field, order_field,
                               variation_field=None, date_field=None, extra_field=None, metric_name=""):
    experiment_name = request.args.get('experiment_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    category = request.args.get('category', '')
    mode = 'heatmap'
    if not experiment_name or not start_date or not end_date:
        return jsonify({"error": "缺少参数"}), 400

    # 步骤1: 优先从缓存中读取预计算好的热力图结果
    cache_engine = get_local_cache_engine()
    cache = get_abtest_cache(
        cache_engine, query_type=mode,
        experiment_name=experiment_name, metric=metric_name, category=category or '',
        start_date=start_date, end_date=end_date
    )
    if cache:
        print(f"HIT CACHE for heatmap: {metric_name}")
        return jsonify(cache)

    # 步骤2: 如果缓存未命中，则执行实时计算
    print(f"MISS CACHE for heatmap: {metric_name}, calculating live.")
    engine = get_db_connection()
    rows = fetch_func(experiment_name, start_date, end_date, engine)
    if not rows:
        return jsonify([])

    df = pd.DataFrame(rows)

    # 增加健壮性检查，防止因数据源问题导致崩溃
    required_cols = ['variation_id', 'cohort_day', value_field]
    if not all(col in df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df.columns]
        return jsonify({"error": f"Data from fetch_func is missing required columns for heatmap: {missing}"}), 500

    # 1. 执行通用的 pivot 操作
    pivoted_table = df.pivot_table(index='variation_id', columns='cohort_day', values=value_field)

    # 2. TypeError 修复: 将所有数字列名强制转换为字符串
    pivoted_table.columns = [str(col) for col in pivoted_table.columns]

    result_df = pivoted_table.reset_index()

    result = result_df.fillna(0).to_dict(orient='records')

    # 将实时计算的结果存入缓存，供下次使用
    set_abtest_cache(
        cache_engine, query_type=mode,
        experiment_name=experiment_name, metric=metric_name, category=category or '',
        start_date=start_date, end_date=end_date, result_json=result
    )
    return jsonify(result)
# ============= 路由注册函数 =============
def make_cohort_trend_api(cfg, metric_name):
    def api_func():
        return generic_cohort_trend_api(
            cfg["fetch_func"], cfg["value_field"], cfg["revenue_field"], cfg["order_field"],
            cfg.get("variation_field"), cfg.get("date_field"), metric_name=metric_name)
    return api_func

def make_cohort_heatmap_api(cfg, metric_name, extra_field=None):
    def api_func():
        return generic_cohort_heatmap_api(
            cfg["fetch_func"], cfg["value_field"], cfg["revenue_field"], cfg["order_field"],
            cfg.get("variation_field"), cfg.get("date_field"), extra_field=cfg.get("extra_field"), metric_name=metric_name)
    return api_func

def register_cohort_routes(bp, config):
    # --- 累计型趋势图 ---
    bp.add_url_rule(
        '/cumulative_retention_trend',
        'cumulative_retention_cohort_trend_api',
        make_cohort_trend_api(config["cumulative_retention"], "cumulative_retention"),
        methods=['GET']
    )
    bp.add_url_rule(
        '/cumulative_ltv_trend',
        'cumulative_ltv_cohort_trend_api',
        make_cohort_trend_api(config["cumulative_ltv"], "cumulative_ltv"),
        methods=['GET']
    )
    bp.add_url_rule(
        '/cumulative_lt_trend',
        'cumulative_lt_cohort_trend_api',
        make_cohort_trend_api(config["cumulative_lt"], "cumulative_lt"),
        methods=['GET']
    )
    # --- 热力图路由（宽表模式） ---
    # bp.add_url_rule(
    #     '/active_retention_d1_heatmap',
    #     'active_retention_d1_heatmap_api',
    #     make_cohort_heatmap_api(config["all_retention_d1"], "all_retention_d1"),
    #     methods=['GET']
    # )
    bp.add_url_rule(
        '/arpu_heatmap',
        'arpu_heatmap_api',
        make_cohort_heatmap_api(config["cohort_arpu"], "cohort_arpu"),
        methods=['GET']
    )
    bp.add_url_rule(
        '/cohort_retention_heatmap',
        'cohort_retention_heatmap_api',
        make_cohort_heatmap_api(config["cohort_retention_heatmap"], "cohort_retention_heatmap"),
        methods=['GET']
    )
    bp.add_url_rule(
        '/time_spend_heatmap',
        'time_spend_heatmap_api',
        make_cohort_heatmap_api(config["cohort_time_spent_heatmap"], "cohort_time_spent_heatmap"),
        methods=['GET']
    )

# 注册蓝图
register_cohort_routes(bp_cohort, INDICATOR_CONFIG)
