import sys
import os
import json
import logging
from collections import defaultdict
from datetime import datetime
import numpy as np

# 自动将项目根目录加入 sys.path，保证绝对导入
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from backend.utils.engine_utils import get_db_connection
from backend.service.config import INDICATOR_CONFIG
from backend.airflow.experiment_filter import get_valid_experiments

# 日志配置
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

def safe_float(val):
    try:
        return float(val)
    except Exception:
        return None

def safe_int(val):
    try:
        return int(val)
    except Exception:
        return None

def bayesian_summary(samples):
    samples = np.array(samples)
    mean = np.mean(samples) if len(samples) > 0 else 0.0
    std = np.std(samples, ddof=1) if len(samples) > 1 else 0.0
    n = len(samples)
    if n < 2 or std == 0:
        posterior_samples = np.array([mean] * 1000)
        ci_lower, ci_upper = mean, mean
    else:
        posterior_samples = np.random.normal(mean, std / np.sqrt(n), 1000)
        ci_lower, ci_upper = np.percentile(posterior_samples, [2.5, 97.5])
    return {
        "mean": float(mean),
        "std": float(std),
        "n": int(n),
        "posterior_samples": [float(x) for x in posterior_samples],
        "credible_interval": [float(ci_lower), float(ci_upper)]
    }

def calc_bayesian_from_snapshot(engine, experiment_name, metric, category, start_date, end_date):
    sql = """
    SELECT variation_id, value, revenue, orders
    FROM abtest_metric_snapshot
    WHERE experiment_name = %s AND metric = %s AND category = %s
    AND event_date >= %s AND event_date <= %s
    """
    with engine.begin() as conn:
        rows = conn.execute(sql, (experiment_name, metric, category, start_date, end_date)).fetchall()
    group_value = defaultdict(list)
    group_revenue = defaultdict(float)
    group_order = defaultdict(int)
    for row in rows:
        v_id = str(row['variation_id'])
        v = row['value']
        revenue = row.get('revenue', 0) if isinstance(row, dict) else row[2] if len(row) > 2 else 0
        order = row.get('orders', 0) if isinstance(row, dict) else row[3] if len(row) > 3 else 0
        if v is not None:
            group_value[v_id].append(safe_float(v))
            group_revenue[v_id] += safe_float(revenue) if revenue is not None else 0.0
            group_order[v_id] += safe_int(order) if order is not None else 0
    result = {
        "groups": [],
        "distribution": {str(k): [safe_float(x) for x in v] for k, v in group_value.items()}
    }
    for v_id, values in group_value.items():
        summary = bayesian_summary(values)
        summary["group"] = v_id
        summary["total_revenue"] = safe_float(group_revenue[v_id])
        summary["total_order"] = safe_int(group_order[v_id])
        result["groups"].append(summary)
    return result

def calc_trend_from_snapshot(engine, experiment_name, metric, category, start_date, end_date):
    sql = """
    SELECT variation_id, event_date, value, revenue, orders
    FROM abtest_metric_snapshot
    WHERE experiment_name = %s AND metric = %s AND category = %s
    AND event_date >= %s AND event_date <= %s
    ORDER BY event_date
    """
    with engine.begin() as conn:
        rows = conn.execute(sql, (experiment_name, metric, category, start_date, end_date)).fetchall()
    date_set = set()
    group_value = defaultdict(dict)
    group_revenue = defaultdict(dict)
    group_order = defaultdict(dict)
    for row in rows:
        v_id = str(row['variation_id'])
        d = str(row['event_date'])[:10]
        value = row['value']
        revenue = row.get('revenue', 0) if isinstance(row, dict) else row[3] if len(row) > 3 else 0
        order = row.get('orders', 0) if isinstance(row, dict) else row[4] if len(row) > 4 else 0
        date_set.add(d)
        group_value[v_id][d] = safe_float(value) if value is not None else None
        group_revenue[v_id][d] = safe_float(revenue) if revenue is not None else None
        group_order[v_id][d] = safe_int(order) if order is not None else None
    dates = sorted(list(date_set))
    series = []
    for v_id in group_value:
        data = [group_value[v_id].get(date, None) for date in dates]
        revenue = [group_revenue[v_id].get(date, None) for date in dates]
        order = [group_order[v_id].get(date, None) for date in dates]
        series.append({
            "variation": v_id,
            "data": data,
            "revenue": revenue,
            "order": order
        })
    return {"dates": dates, "series": series}

def write_to_query_cache(engine, query_type, experiment_name, metric, category, start_date, end_date, result_json):
    sql = """
    INSERT INTO abtest_query_cache
    (query_type, experiment_name, metric, category, start_date, end_date, updated_at, result_json)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    with engine.begin() as conn:
        conn.execute(sql, (
            query_type, experiment_name, metric, category,
            start_date, end_date, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            json.dumps(result_json, default=str)
        ))

def persist_all_results_for_experiment(engine, experiment, metrics_config):
    experiment_name = experiment["experiment_name"]
    start_date = experiment["phase_start_time"][:10]
    end_date = experiment["phase_end_time"][:10]
    logger.info(f"\n==== 处理实验: {experiment_name} ({start_date} ~ {end_date}) ====")
    for metric, cfg in metrics_config.items():
        category = cfg.get("category", "")
        # 贝叶斯统计
        bayes_result = calc_bayesian_from_snapshot(engine, experiment_name, metric, category, start_date, end_date)
        write_to_query_cache(engine, "bayesian", experiment_name, metric, category, start_date, end_date, bayes_result)
        logger.info(f"[{metric}] 贝叶斯聚合: {len(bayes_result['groups'])} variations")
        for group in bayes_result["groups"]:
            logger.info(f"  - group={group['group']}, mean={group['mean']:.4f}, 95%CI={group['credible_interval']}, n={group['n']}")
        # 趋势图
        trend_result = calc_trend_from_snapshot(engine, experiment_name, metric, category, start_date, end_date)
        write_to_query_cache(engine, "trend", experiment_name, metric, category, start_date, end_date, trend_result)
        logger.info(f"[{metric}] 趋势聚合: {len(trend_result['series'])} variations, {len(trend_result['dates'])} dates")
        for s in trend_result['series']:
            logger.info(f"  - var={s['variation']} trend={s['data'][:3]}... (共{len(s['data'])}天)")

def main():
    engine = get_db_connection()
    experiments = get_valid_experiments()
    logger.info(f"\n共获取到 {len(experiments)} 个实验，依次处理...\n")
    for exp in experiments:
        persist_all_results_for_experiment(engine, exp, INDICATOR_CONFIG)

if __name__ == "__main__":
    main()
