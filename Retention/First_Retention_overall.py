import sys
import urllib.parse
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
import warnings

warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

# ============= 数据库连接 =============
import logging
import os
from dotenv import load_dotenv
load_dotenv()

def get_db_connection():
    password = urllib.parse.quote_plus(os.environ['DB_PASSWORD'])
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    logging.info("✅ 数据库连接已建立。")
    return engine

# ============= 从宽表提取数据 =============
def extract_data_from_db(tag, engine):
    query = f"SELECT * FROM tbl_wide_user_retention_{tag};"
    try:
        df = pd.read_sql(query, engine)
        # 转成 datetime 类型，方便后续日期过滤
        df['dt'] = pd.to_datetime(df['dt'])
        if "new_users" in df.columns:
            df.rename(columns={"new_users": "users"}, inplace=True)
        return df.fillna(0)
    except Exception as e:
        print(f"数据提取失败: {e}")
        return None

# ============= 计算留存率及置信区间 =============
def calculate_retention(df):
    days = {"d1": 1, "d3": 3, "d7": 7, "d15": 15}
    results = []
    df = df[df["users"] > 0].copy()
    for _, row in df.iterrows():
        dt = row["dt"]
        try:
            variation = int(row["variation"])
        except:
            variation = row["variation"]
        users = row["users"]
        cov = row.get("coverage_ratio", None)
        for day_key, day in days.items():
            if day_key not in row:
                continue
            retained = row[day_key]
            retention_rate = retained / users if users > 0 else 0
            se = np.sqrt(retention_rate * (1 - retention_rate) / users) if users > 0 else 0
            ci_lower = max(0, retention_rate - 1.96 * se)
            ci_upper = min(1, retention_rate + 1.96 * se)
            results.append({
                "dt": dt,
                "variation": variation,
                "day": day,
                "users": int(users),
                "retained": int(retained),
                "retention_rate": retention_rate,
                "ci_lower": ci_lower,
                "ci_upper": ci_upper,
                "coverage_ratio": cov
            })
    return pd.DataFrame(results)

# ============= 新整体留存率 + uplift（贝叶斯和全量频率法）+ 胜率计算并写入 =============
def calculate_overall_day_metrics_and_save(retention_df, engine, tag, days=(1, 3, 7, 15), n_samples=10000):
    table_name = f"tbl_report_user_retention_{tag}_overall"
    results = []

    max_dt = retention_df['dt'].max()

    for day in days:
        day_data = retention_df[retention_df["day"] == day].copy()
        if day_data.empty:
            print(f"❌ 没有 day={day} 的数据，无法计算整体留存")
            continue

        unique_dates = sorted(day_data["dt"].unique())
        if len(unique_dates) > 2:
            day_data = day_data[~day_data["dt"].isin([unique_dates[0], unique_dates[-1]])]

        # 过滤掉注册日期晚于 max_dt - day 的数据，因为这些用户还没满day天
        cutoff = max_dt - timedelta(days=day)
        day_data = day_data[day_data['dt'] <= cutoff]
        if day_data.empty:
            print(f"⚠️ 过滤后 day={day} 无可用数据，跳过")
            continue

        grouped = day_data.groupby("variation", as_index=False).agg({
            "users": "sum",
            "retained": "sum"
        })

        control = grouped[grouped["variation"] == 0]
        if control.empty:
            print(f"❌ day={day} 未找到对照组（variation=0）")
            continue
        control = control.iloc[0]

        alpha_c = control["retained"] + 1
        beta_c = control["users"] - control["retained"] + 1
        samples_c = np.random.beta(alpha_c, beta_c, n_samples)
        mean_c = samples_c.mean()
        freq_c = control["retained"] / control["users"]

        exp_users_total = grouped[grouped["variation"] != 0]["users"].sum()
        exp_retained_total = grouped[grouped["variation"] != 0]["retained"].sum()
        control_users_total = control["users"]
        control_retained_total = control["retained"]

        exp_rate = exp_retained_total / exp_users_total if exp_users_total > 0 else 0
        control_rate = control_retained_total / control_users_total if control_users_total > 0 else 0
        freq_uplift = (exp_rate - control_rate) / control_rate if control_rate > 0 else 0

        for _, row in grouped[grouped["variation"] != 0].iterrows():
            var = int(row["variation"])
            alpha_e = row["retained"] + 1
            beta_e = row["users"] - row["retained"] + 1
            samples_e = np.random.beta(alpha_e, beta_e, n_samples)
            mean_e = samples_e.mean()
            freq_e = row["retained"] / row["users"]
            uplift = (mean_e - mean_c) / mean_c if mean_c > 0 else 0
            chance_to_win = np.mean(samples_e > samples_c)

            results.append({
                "day": day,
                "variation": var,
                "control_users": int(control["users"]),
                "control_retained": int(control["retained"]),
                "control_freq_rate": round(freq_c, 6),
                "control_bayes_rate": round(mean_c, 6),
                "exp_users": int(row["users"]),
                "exp_retained": int(row["retained"]),
                "exp_freq_rate": round(freq_e, 6),
                "exp_bayes_rate": round(mean_e, 6),
                f"overall_d{day}_uplift": round(uplift, 6),
                f"overall_chance_to_win": round(chance_to_win, 6),
                "freq_uplift": round(freq_uplift, 6)
            })

    df_result = pd.DataFrame(results)

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        day INT,
        variation INT,
        control_users INT,
        control_retained INT,
        control_freq_rate DOUBLE,
        control_bayes_rate DOUBLE,
        exp_users INT,
        exp_retained INT,
        exp_freq_rate DOUBLE,
        exp_bayes_rate DOUBLE,
        overall_d1_uplift DOUBLE,
        overall_d3_uplift DOUBLE,
        overall_d7_uplift DOUBLE,
        overall_d15_uplift DOUBLE,
        overall_chance_to_win DOUBLE,
        freq_uplift DOUBLE
    ) ENGINE=OLAP
    DUPLICATE KEY(day, variation)
    DISTRIBUTED BY HASH(day, variation) BUCKETS 10
    PROPERTIES (
        "replication_num" = "3"
    );
    """

    try:
        with engine.connect() as conn:
            conn.execute(text("SET query_timeout = 30000;"))
            conn.execute(text(create_table_query))
            conn.execute(text(f"TRUNCATE TABLE {table_name}"))
        print(f"✅ 表 {table_name} 已创建并清空")

        for day in days:
            uplift_col = f"overall_d{day}_uplift"
            if uplift_col not in df_result.columns:
                df_result[uplift_col] = None
        if "freq_uplift" not in df_result.columns:
            df_result["freq_uplift"] = None

        df_result.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=500,
            dtype={
                'day': sqlalchemy.Integer(),
                'variation': sqlalchemy.Integer(),
                'control_users': sqlalchemy.Integer(),
                'control_retained': sqlalchemy.Integer(),
                'control_freq_rate': sqlalchemy.Float(),
                'control_bayes_rate': sqlalchemy.Float(),
                'exp_users': sqlalchemy.Integer(),
                'exp_retained': sqlalchemy.Integer(),
                'exp_freq_rate': sqlalchemy.Float(),
                'exp_bayes_rate': sqlalchemy.Float(),
                'overall_d1_uplift': sqlalchemy.Float(),
                'overall_d3_uplift': sqlalchemy.Float(),
                'overall_d7_uplift': sqlalchemy.Float(),
                'overall_d15_uplift': sqlalchemy.Float(),
                'overall_chance_to_win': sqlalchemy.Float(),
                'freq_uplift': sqlalchemy.Float()
            }
        )
        print(f"📊 整体留存结果（多天）已写入表 {table_name}！")
        print(df_result)
    except Exception as e:
        print(f"❌ 写入 {table_name} 失败: {e}")

# ============= 主流程 =============
def main(tag):
    engine = get_db_connection()
    df = extract_data_from_db(tag, engine)
    if df is None:
        print("❌ 数据提取失败")
        return

    retention_df = calculate_retention(df)
    calculate_overall_day_metrics_and_save(retention_df, engine, tag, days=[1, 3, 7, 15])

if __name__ == "__main__":
    tag = "mobile"
    main(tag)
