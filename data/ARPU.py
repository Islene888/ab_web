import logging
import os
import urllib.parse
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import numpy as np
from flask import Flask, request, jsonify
import warnings
import sys
warnings.filterwarnings("ignore", category=FutureWarning)
load_dotenv()

app = Flask(__name__)

def get_db_connection():
    password = urllib.parse.quote_plus(os.environ.get('DB_PASSWORD', 'flowgpt@2024.com'))
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    logging.info("✅ 数据库连接已建立。")
    return engine

def fetch_group_arpu_samples(experiment_name, start_date, end_date, engine):
    query = f'''
    WITH
        exp AS (
            SELECT user_id, variation_id, event_date
            FROM (
                SELECT
                    user_id,
                    variation_id,
                    event_date,
                    ROW_NUMBER() OVER (PARTITION BY user_id, event_date ORDER BY event_date DESC) AS rn
                FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                WHERE experiment_id = '{experiment_name}'
                    AND event_date BETWEEN '{start_date}' AND '{end_date}'
            ) t
            WHERE rn = 1
        ),
        daily_active AS (
            SELECT
                e.event_date,
                e.variation_id,
                COUNT(DISTINCT pv.user_id) AS active_users
            FROM flow_event_info.tbl_app_event_page_view pv
            JOIN exp e ON pv.user_id = e.user_id AND pv.event_date = e.event_date
            GROUP BY e.event_date, e.variation_id
        ),
        sub AS (
            SELECT user_id, event_date, SUM(revenue) AS sub_revenue
            FROM flow_event_info.tbl_app_event_subscribe
            WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY user_id, event_date
        ),
        ord AS (
            SELECT user_id, event_date, SUM(revenue) AS order_revenue
            FROM flow_event_info.tbl_app_event_currency_purchase
            WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY user_id, event_date
        ),
        user_revenue AS (
            SELECT
                e.event_date,
                e.variation_id,
                COALESCE(s.sub_revenue, 0) + COALESCE(o.order_revenue, 0) AS total_revenue
            FROM exp e
            LEFT JOIN sub s ON e.user_id = s.user_id AND e.event_date = s.event_date
            LEFT JOIN ord o ON e.user_id = o.user_id AND e.event_date = o.event_date
        ),
        group_revenue AS (
            SELECT
                event_date,
                variation_id,
                SUM(total_revenue) AS revenue
            FROM user_revenue
            GROUP BY event_date, variation_id
        ),
        daily_ad AS (
            SELECT event_date, SUM(ad_revenue) AS ad_revenue
            FROM flow_event_info.tbl_app_event_ads_impression
            WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY event_date
        ),
        daily_total_active AS (
            SELECT event_date, SUM(active_users) AS total_active
            FROM daily_active
            GROUP BY event_date
        )
    SELECT
        da.variation_id,
        da.event_date,
        COALESCE(gr.revenue, 0)
            + COALESCE(dad.ad_revenue, 0) * da.active_users / NULLIF(dta.total_active, 0)
            AS total_revenue,
        da.active_users,
        ROUND(
            (
                COALESCE(gr.revenue, 0)
                + COALESCE(dad.ad_revenue, 0) * da.active_users / NULLIF(dta.total_active, 0)
            ) / NULLIF(da.active_users, 0),
        4) AS arpu
    FROM daily_active da
    LEFT JOIN group_revenue gr
        ON da.event_date = gr.event_date AND da.variation_id = gr.variation_id
    LEFT JOIN daily_ad dad
        ON da.event_date = dad.event_date
    LEFT JOIN daily_total_active dta
        ON da.event_date = dta.event_date
    WHERE da.event_date >= '{start_date}' AND da.event_date <= '{end_date}';
    '''
    with engine.connect() as conn:
        df = conn.execute(text(query)).fetchall()
    print(f"实验 {experiment_name} 查询到 {len(df)} 条记录")
    print(df)
    return df

def bayesian_summary(arpu_samples):
    samples = np.array(arpu_samples)
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

@app.route('/api/arpu_bayesian', methods=['GET'])
def arpu_bayesian_api():
    experiment_name = request.args.get('experiment_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not experiment_name or not start_date or not end_date:
        return jsonify({"error": "请提供 experiment_name, start_date, end_date 参数"}), 400
    engine = get_db_connection()
    rows = fetch_group_arpu_samples(experiment_name, start_date, end_date, engine)
    group_dict = {}
    group_revenue = {}
    group_order = {}
    for row in rows:
        variation_id = row[0]
        arpu = row[4]
        revenue = row[2]
        active_users = row[3]
        if arpu is not None:
            group_dict.setdefault(variation_id, []).append(float(arpu))
            group_revenue[variation_id] = group_revenue.get(variation_id, 0) + float(revenue)
            group_order[variation_id] = group_order.get(variation_id, 0) + int(active_users)
    result = {
        "groups": [],
        "distribution": group_dict
    }
    for group, arpu_list in group_dict.items():
        summary = bayesian_summary(arpu_list)
        summary["group"] = group
        summary["total_revenue"] = group_revenue.get(group, 0)
        summary["total_order"] = group_order.get(group, 0)
        result["groups"].append(summary)
    return jsonify(result)

@app.route('/api/arpu_trend', methods=['GET'])
def arpu_trend_api():
    experiment_name = request.args.get('experiment_name')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not experiment_name or not start_date or not end_date:
        return "参数缺失", 400
    engine = get_db_connection()
    rows = fetch_group_arpu_samples(experiment_name, start_date, end_date, engine)
    from collections import defaultdict
    date_set = set()
    group_arpu = defaultdict(dict)
    group_revenue = defaultdict(dict)
    group_order = defaultdict(dict)
    for row in rows:
        variation_id, event_date, revenue, active_users, arpu = row
        date_str = str(event_date)
        date_set.add(date_str)
        group_arpu[variation_id][date_str] = float(arpu) if arpu is not None else None
        group_revenue[variation_id][date_str] = float(revenue)
        group_order[variation_id][date_str] = int(active_users)
    dates = sorted(list(date_set))
    series = []
    for group in group_arpu:
        data = [group_arpu[group].get(date, None) for date in dates]
        revenue = [group_revenue[group].get(date, None) for date in dates]
        order = [group_order[group].get(date, None) for date in dates]
        series.append({
            "variation": group,
            "data": data,
            "revenue": revenue,
            "order": order
        })
    return jsonify({"dates": dates, "series": series})

# if __name__ == "__main__":
#     app.run(host="0.0.0.0", port=5050, debug=True)
