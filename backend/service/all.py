from flask import Blueprint, request, jsonify
from collections import defaultdict
from .config import INDICATOR_CONFIG
from functools import lru_cache

from .service import bayesian_summary
from ..utils.cache_utils import get_abtest_cache, set_abtest_cache
from ..utils.engine_utils import get_local_cache_engine, get_db_connection

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
    import traceback
    try:
        experiment_name = request.args.get('experiment_name')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        category = request.args.get('category')

        if not experiment_name or not start_date or not end_date or not category:
            return jsonify({"error": "参数缺失"}), 400

        metric_names = get_metric_names(category)
        if not metric_names:
            return jsonify({"error": "未知的类别"}), 400

        local_engine = get_local_cache_engine()
        engine = get_db_connection()
        all_results = {}

        for metric in metric_names:
            cfg = INDICATOR_CONFIG.get(metric)
            if not cfg:
                continue
            true_category = cfg.get("category", "") or ""
            # 查 metric 级别缓存（query_type="trend"，category=真实分类）
            cache = get_abtest_cache(
                local_engine, "trend", experiment_name, metric, true_category, start_date, end_date
            )
            if cache:
                all_results[metric] = cache
                continue

            try:
                rows = cfg["fetch_func"](experiment_name, start_date, end_date, engine)
            except Exception as e:
                continue

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
            # 单独缓存每个 metric
            set_abtest_cache(
                local_engine, "trend", experiment_name, metric, true_category, start_date, end_date, all_results[metric]
            )

        return jsonify(all_results)
    except Exception as e:
        import traceback
        print("接口整体报错:", e)
        print(traceback.format_exc())
        return jsonify({"error": "内部服务器错误", "msg": str(e)}), 500

@all_bp.route('/api/all_bayesian', methods=['GET'])
def all_bayesian():
    import traceback
    try:
        experiment_name = request.args.get('experiment_name')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        category = request.args.get('category')

        if not all([experiment_name, start_date, end_date, category]):
            return jsonify({"error": "参数缺失，请提供 experiment_name, start_date, end_date 和 category"}), 400

        local_engine = get_local_cache_engine()
        engine = get_db_connection()
        all_results = {}

        metric_names = get_metric_names(category)
        if not metric_names:
            return jsonify({"error": f"未知的类别: {category}"}), 400

        for metric in metric_names:
            cfg = INDICATOR_CONFIG.get(metric)
            if not cfg:
                continue
            true_category = cfg.get("category", "") or ""
            # 查 metric 级别缓存（query_type="bayesian"，category=真实分类）
            cache = get_abtest_cache(
                local_engine, "bayesian", experiment_name, metric, true_category, start_date, end_date
            )
            if cache:
                all_results[metric] = cache
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
            all_results[metric] = {
                "groups": metric_groups_summary,
                "distribution": {str(k): v for k, v in group_dict.items()}
            }
            # 单独缓存每个 metric
            set_abtest_cache(
                local_engine, "bayesian", experiment_name, metric, true_category, start_date, end_date, all_results[metric]
            )

        return jsonify(all_results)
    except Exception as e:
        import traceback
        print("all_bayesian 报错:", e)
        print(traceback.format_exc())
        return jsonify({"error": "内部服务器错误", "msg": str(e)}), 500
