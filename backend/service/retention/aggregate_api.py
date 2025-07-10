from flask import request, jsonify
from backend.service.service import app, get_db_connection, bayesian_summary
from backend.sql_jobs.Retention.active_retention import fetch_active_user_retention
from backend.sql_jobs.Retention.new_retention import fetch_new_user_retention
import numpy as np
from datetime import datetime, timedelta

def get_retention_fetcher(user_type):
    if user_type == "new":
        return fetch_new_user_retention
    else:
        return fetch_active_user_retention

def fetch_retention_d(engine, experiment_name, start_time, end_time, day, fetcher):
    rows = fetcher(engine, experiment_name, start_time, end_time)
    # new_users, d1, d3, d7, d15 索引和 active_users, d1_users, ... 索引要统一
    # day=1: d1, day=3: d3, ...
    if fetcher == fetch_new_user_retention:
        # (date, variation, new_users, d1, d3, d7, d15, total_assigned)
        # 0:date, 1:variation, 2:new_users, 3:d1, 4:d3, 5:d7, 6:d15, 7:total_assigned
        if day == 1:
            return [(row[1], row[0], row[3]/row[2] if row[2] else 0, row[2], row[3]) for row in rows]
        if day == 3:
            return [(row[1], row[0], row[4]/row[2] if row[2] else 0, row[2], row[4]) for row in rows]
        if day == 7:
            return [(row[1], row[0], row[5]/row[2] if row[2] else 0, row[2], row[5]) for row in rows]
        if day == 15:
            return [(row[1], row[0], row[6]/row[2] if row[2] else 0, row[2], row[6]) for row in rows]
    else:
        # (活跃用户留存的原结构)
        if day == 1:
            return [(row[1], row[0], row[7], row[2], row[3]) for row in rows]
        if day == 3:
            return [(row[1], row[0], row[8], row[2], row[4]) for row in rows]
        if day == 7:
            return [(row[1], row[0], row[9], row[2], row[5]) for row in rows]
        if day == 15:
            return [(row[1], row[0], row[10], row[2], row[6]) for row in rows]

def group_bayes(rows, user_idx=None, active_idx=None):
    from collections import defaultdict
    # 仅在留存率类（传入user_idx和active_idx）时过滤掉分母>0但分子为0的天
    if user_idx is not None and active_idx is not None:
        filtered_rows = [row for row in rows if row[user_idx] > 0 and row[2] > 0]
    else:
        filtered_rows = rows
    group_dict = defaultdict(list)
    group_users = defaultdict(int)
    group_active = defaultdict(int)
    for row in filtered_rows:
        group_dict[row[0]].append(float(row[2]))  # 0: variation, 2: value_field
        if user_idx is not None and active_idx is not None:
            group_users[row[0]] += row[user_idx] if row[user_idx] is not None else 0
            group_active[row[0]] += row[active_idx] if row[active_idx] is not None else 0
    result = []
    for group, value_list in group_dict.items():
        summary = bayesian_summary(value_list)
        summary["group"] = group
        summary["mean"] = float(np.mean(value_list))  # 算术平均
        if user_idx is not None and active_idx is not None:
            summary["numerator"] = group_users[group]
            summary["denominator"] = group_active[group]
        result.append(summary)
    return result

@app.route('/api/all_retention_bayesian', methods=['GET'])
def all_retention_bayesian():
    user_type = request.args.get('user_type', 'active')
    experiment_name = request.args.get('experiment_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not experiment_name or not start_date or not end_date:
        return jsonify({"error": "请提供 experiment_name, start_date, end_date 参数"}), 400
    engine = get_db_connection()
    fetcher = get_retention_fetcher(user_type)
    d1 = fetch_retention_d(engine, experiment_name, start_date, end_date, 1, fetcher)
    d3 = fetch_retention_d(engine, experiment_name, start_date, end_date, 3, fetcher)
    d7 = fetch_retention_d(engine, experiment_name, start_date, end_date, 7, fetcher)
    d15 = fetch_retention_d(engine, experiment_name, start_date, end_date, 15, fetcher)
    result = {
        "d1": group_bayes(d1, user_idx=4, active_idx=3),
        "d3": group_bayes(d3, user_idx=4, active_idx=3),
        "d7": group_bayes(d7, user_idx=4, active_idx=3),
        "d15": group_bayes(d15, user_idx=4, active_idx=3)
    }
    return jsonify(result)

@app.route('/api/all_retention_trend', methods=['GET'])
def all_retention_trend():
    user_type = request.args.get('user_type', 'active')
    experiment_name = request.args.get('experiment_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not experiment_name or not start_date or not end_date:
        return "参数缺失", 400
    engine = get_db_connection()
    fetcher = get_retention_fetcher(user_type)
    rows = fetcher(engine, experiment_name, start_date, end_date)
    from collections import defaultdict
    date_set = set()
    group_value = {"d1": defaultdict(dict), "d3": defaultdict(dict), "d7": defaultdict(dict), "d15": defaultdict(dict)}
    if fetcher == fetch_new_user_retention:
        for row in rows:
            group = row[1]
            date_str = str(row[0])
            date_set.add(date_str)
            group_value["d1"][group][date_str] = float(row[3])/row[2] if row[2] else None
            group_value["d3"][group][date_str] = float(row[4])/row[2] if row[2] else None
            group_value["d7"][group][date_str] = float(row[5])/row[2] if row[2] else None
            group_value["d15"][group][date_str] = float(row[6])/row[2] if row[2] else None
    else:
        for row in rows:
            group = row[1]
            date_str = str(row[0])
            date_set.add(date_str)
            group_value["d1"][group][date_str] = float(row[7]) if row[7] is not None else None
            group_value["d3"][group][date_str] = float(row[8]) if row[8] is not None else None
            group_value["d7"][group][date_str] = float(row[9]) if row[9] is not None else None
            group_value["d15"][group][date_str] = float(row[10]) if row[10] is not None else None
    dates = sorted(list(date_set))
    if len(dates) > 1:
        dates = dates[:-1]
    result = {}
    for key in ["d1", "d3", "d7", "d15"]:
        series = []
        for group in group_value[key]:
            data = [group_value[key][group].get(date, None) for date in dates]
            series.append({
                "variation": group,
                "data": data
            })
        result[key] = {"dates": dates, "series": series}
    return jsonify(result)

@app.route('/api/new_retention_bayesian', methods=['GET'])
def new_retention_bayesian():
    experiment_name = request.args.get('experiment_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not experiment_name or not start_date or not end_date:
        return jsonify({"error": "请提供 experiment_name, start_date, end_date 参数"}), 400
    engine = get_db_connection()
    rows = fetch_new_user_retention(engine, experiment_name, start_date, end_date)
    d1 = [(row[1], row[0], row[3]/row[2] if row[2] else 0, row[2], row[3]) for row in rows]
    d3 = [(row[1], row[0], row[4]/row[2] if row[2] else 0, row[2], row[4]) for row in rows]
    d7 = [(row[1], row[0], row[5]/row[2] if row[2] else 0, row[2], row[5]) for row in rows]
    d15 = [(row[1], row[0], row[6]/row[2] if row[2] else 0, row[2], row[6]) for row in rows]
    result = {
        "d1": group_bayes(d1, user_idx=4, active_idx=3),
        "d3": group_bayes(d3, user_idx=4, active_idx=3),
        "d7": group_bayes(d7, user_idx=4, active_idx=3),
        "d15": group_bayes(d15, user_idx=4, active_idx=3)
    }
    return jsonify(result)

@app.route('/api/new_retention_trend', methods=['GET'])
def new_retention_trend():
    experiment_name = request.args.get('experiment_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not experiment_name or not start_date or not end_date:
        return "参数缺失", 400
    engine = get_db_connection()
    rows = fetch_new_user_retention(engine, experiment_name, start_date, end_date)
    from collections import defaultdict
    date_set = set()
    group_value = {"d1": defaultdict(dict), "d3": defaultdict(dict), "d7": defaultdict(dict), "d15": defaultdict(dict)}
    for row in rows:
        group = row[1]
        date_str = str(row[0])
        date_set.add(date_str)
        group_value["d1"][group][date_str] = float(row[3])/row[2] if row[2] else None
        group_value["d3"][group][date_str] = float(row[4])/row[2] if row[2] else None
        group_value["d7"][group][date_str] = float(row[5])/row[2] if row[2] else None
        group_value["d15"][group][date_str] = float(row[6])/row[2] if row[2] else None
    dates = sorted(list(date_set))
    if len(dates) > 1:
        dates = dates[:-1]
    result = {}
    for key in ["d1", "d3", "d7", "d15"]:
        series = []
        for group in group_value[key]:
            data = [group_value[key][group].get(date, None) for date in dates]
            series.append({
                "variation": group,
                "data": data
            })
        result[key] = {"dates": dates, "series": series}
    return jsonify(result)