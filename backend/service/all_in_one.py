from flask import Blueprint, request, jsonify
from collections import defaultdict
from ..service.config import INDICATOR_CONFIG
import traceback

from ..service.service import bayesian_summary
from ..utils.cache_utils import set_abtest_cache, get_abtest_cache
from ..utils.engine_utils import get_db_connection, get_local_cache_engine

bp = Blueprint("all_in_one", __name__)

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

@bp.route('/api/all_category_all_metrics', methods=['GET'])
def all_category_all_metrics():
    experiment_name = request.args.get('experiment_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not experiment_name or not start_date or not end_date:
        return jsonify({"error": "缺少参数"}), 400

    local_engine = get_local_cache_engine()
    engine = get_db_connection()
    all_results = {}

    for metric_name, cfg in INDICATOR_CONFIG.items():
        # ---- 取 config 里的真实分类 ----
        category = cfg.get("category", "") or ""
        # ---- 优先查和单指标一致的缓存（query_type="bayesian"，category=真实分类） ----
        cache = get_abtest_cache(
            local_engine, "bayesian", experiment_name, metric_name, category, start_date, end_date
        )
        if cache:
            all_results[metric_name] = cache
            continue

        fetch_func = cfg["fetch_func"]
        try:
            rows = fetch_func(experiment_name, start_date, end_date, engine)
            group_dict = defaultdict(list)
            group_revenue = defaultdict(float)
            group_order = defaultdict(int)

            for row in rows:
                v_idx = getval(row, cfg["variation_field"])
                value = getval(row, cfg["value_field"])
                revenue = getval(row, cfg["revenue_field"])
                order = getval(row, cfg["order_field"])
                if v_idx is None or value is None:
                    continue
                group_dict[v_idx].append(float(value))
                group_revenue[v_idx] += float(revenue) if revenue is not None else 0
                group_order[v_idx] += int(order) if order is not None else 0

            metric_result = {
                "groups": [],
                "distribution": {str(k): v for k, v in group_dict.items()} if group_dict else {}
            }
            for group, value_list in group_dict.items():
                if not value_list:
                    continue
                summary = bayesian_summary(value_list)
                summary["group"] = group
                summary["total_revenue"] = group_revenue[group]
                summary["total_order"] = group_order[group]
                metric_result["groups"].append(summary)

            all_results[metric_name] = metric_result

            # ---- 写入缓存（和单指标接口完全一致） ----
            try:
                set_abtest_cache(
                    engine_local=local_engine,
                    query_type="bayesian",
                    experiment_name=experiment_name,
                    metric=metric_name,
                    category=category,
                    start_date=start_date,
                    end_date=end_date,
                    result_json=metric_result,
                )
            except Exception as e:
                print(f"abtest cache persist error for {metric_name}: {e}")

        except Exception as e:
            all_results[metric_name] = {
                "groups": [],
                "distribution": {},
                "error": str(e),
                "traceback": traceback.format_exc()
            }

    return jsonify(all_results)
