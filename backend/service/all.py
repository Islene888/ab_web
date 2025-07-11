# backend/service/all.py
from flask import Blueprint, request, jsonify
from collections import defaultdict
from backend.service.service import get_db_connection, bayesian_summary
from backend.service.config import INDICATOR_CONFIG


all_bp = Blueprint("all", __name__)


CATEGORY_METRIC_MAP = {
    "business": ['aov', 'arpu', 'arppu', 'subscribe_rate', 'payment_rate_all', 'payment_rate_new', 'ltv', 'cancel_sub', 'first_new_sub', 'recharge_rate'],
    "engagement": ['continue', 'conversation_reset', 'edit', 'follow', 'message', 'new_conversation', 'regen'],
    "retention": ['all_retention', 'new_retention'],
    "chat": ['click_rate', 'explore_start_chat_rate', 'avg_chat_rounds', 'avg_start_chat_bots', 'avg_click_bots', 'avg_time_spent', 'explore_click_rate', 'explore_avg_chat_rounds'],
}

@all_bp.route('/api/all_trend', methods=['GET'])
def all_trend():
    experiment_name = request.args.get('experiment_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    category = request.args.get('category')

    if not experiment_name or not start_date or not end_date or not category:
        return jsonify({"error": "参数缺失"}), 400

    metric_names = CATEGORY_METRIC_MAP.get(category, [])
    if not metric_names:
        return jsonify({"error": "未知的类别"}), 400

    all_results = {}
    engine = get_db_connection()  # 只初始化一次连接

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
    return jsonify(all_results)



# === 新增的API函数 ===
@all_bp.route('/api/all_bayesian', methods=['GET'])
def all_bayesian():
    # 1. 获取和验证请求参数 (这部分不变)
    experiment_name = request.args.get('experiment_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    category = request.args.get('category')

    if not all([experiment_name, start_date, end_date, category]):
        return jsonify({"error": "参数缺失，请提供 experiment_name, start_date, end_date 和 category"}), 400

    metric_names = CATEGORY_METRIC_MAP.get(category, [])
    if not metric_names:
        return jsonify({"error": f"未知的类别: {category}"}), 400

    all_results = {}
    engine = get_db_connection()

    # --- BUG 修复：在这里定义 getval 辅助函数 ---
    def getval(row, field):
        if isinstance(field, int):
            try:
                return row[field]
            except (IndexError, TypeError):
                return None
        elif isinstance(field, str):
            if isinstance(row, dict):
                return row.get(field)
        return None

    # --- 修复结束 ---

    for metric in metric_names:
        cfg = INDICATOR_CONFIG.get(metric)
        if not cfg:
            continue

        rows = cfg["fetch_func"](experiment_name, start_date, end_date, engine)
        group_dict = defaultdict(list)
        group_revenue = defaultdict(float)
        group_order = defaultdict(int)

        for row in rows:
            # --- BUG 修复：使用 getval 函数获取数据 ---
            variation_id = getval(row, cfg.get("variation_field", 0))
            value = getval(row, cfg.get("value_field"))
            revenue = getval(row, cfg.get("revenue_field"))
            order = getval(row, cfg.get("order_field"))
            # --- 修复结束 ---

            if value is not None and variation_id is not None:
                group_dict[variation_id].append(float(value))
                group_revenue[variation_id] += float(revenue or 0)
                group_order[variation_id] += int(order or 0)

        metric_groups_summary = []
        for group, value_list in group_dict.items():
            if not value_list: continue
            summary = bayesian_summary(value_list)
            summary["group"] = group
            summary["total_revenue"] = group_revenue[group]
            summary["total_order"] = group_order[group]
            metric_groups_summary.append(summary)

        all_results[metric] = {"groups": metric_groups_summary}

    return jsonify(all_results)