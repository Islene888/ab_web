import logging
import json
from collections import defaultdict
from datetime import datetime
from inspect import signature
import pandas as pd
from sqlalchemy.sql import text

# 确保以下导入路径根据您的项目结构是正确的
from backend.service.config import INDICATOR_CONFIG
from backend.utils.engine_utils import get_db_connection
from backend.airflow.experiment_filter import get_valid_experiments

# 【新增】从您另一个脚本中导入缓存写入函数，如果它在不同位置，请调整路径
try:
    from backend.airflow.summary_cache import write_to_query_cache
except ImportError:
    # 如果无法导入，提供一个本地的备用定义
    def write_to_query_cache(engine_or_conn, query_type, experiment_name, metric, category, start_date, end_date,
                             result_json):
        sql = """
        INSERT INTO abtest_query_cache
        (query_type, experiment_name, metric, category, start_date, end_date, updated_at, result_json)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        updated_at = VALUES(updated_at),
        result_json = VALUES(result_json)
        """
        # 兼容传入的是 engine 还是 connection
        if hasattr(engine_or_conn, 'execute'):
            conn = engine_or_conn
            conn.execute(sql, (
                query_type, experiment_name, metric, category,
                start_date, end_date, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                json.dumps(result_json, default=str)
            ))
        else:  # Assumes it's an engine
            with engine_or_conn.begin() as conn:
                conn.execute(sql, (
                    query_type, experiment_name, metric, category,
                    start_date, end_date, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    json.dumps(result_json, default=str)
                ))

logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)


# getval 和 call_fetch_func_compatible 函数保持不变
def getval(r, field):
    if field is None:
        return None
    if isinstance(field, int):
        try:
            return r[field]
        except (IndexError, TypeError):
            return None
    # 修正：当r是SQLAlchemy的RowProxy时，它不是dict的实例，但可以用key访问
    elif isinstance(field, str):
        try:
            return r[field]
        except (KeyError, TypeError):
            return None
    return None


def call_fetch_func_compatible(fetch_func, experiment_name, start_date, end_date, engine_or_conn, *args, **kwargs):
    # 该函数保持不变
    if hasattr(engine_or_conn, "connect") and callable(engine_or_conn.connect):
        return fetch_func(experiment_name, start_date, end_date, engine_or_conn, *args, **kwargs)
    else:
        # 创建一个模拟引擎以兼容接收 connection 的情况
        class FakeEngine:
            def connect(self_inner):
                # 确保 with 语句可以工作
                class FakeConnection:
                    def __enter__(self): return engine_or_conn

                    def __exit__(self, type, value, traceback): pass

                return FakeConnection()

        return fetch_func(experiment_name, start_date, end_date, FakeEngine(), *args, **kwargs)


def run_all_metrics(experiment_name, start_date, end_date, engine=None):
    """
    【重构版】
    遍历所有指标配置。
    - 如果是热力图指标，直接计算并缓存最终结果。
    - 如果是趋势图指标，将原始数据存入快照表。
    """
    if engine is None:
        engine = get_db_connection()

    with engine.begin() as conn:
        for metric, cfg in INDICATOR_CONFIG.items():
            logger.info("=" * 60)
            logger.info(f"Running metric: {metric}  |  Category: {cfg.get('category', '')}")

            fetch_func = cfg["fetch_func"]

            # --- 核心修改：根据指标类型选择不同的处理路径 ---
            if cfg.get("result_type") == "heatmap":
                # --- 新逻辑：处理热力图指标 ---
                try:
                    logger.info(f"Processing as HEATMAP: {metric}")
                    rows = call_fetch_func_compatible(fetch_func, experiment_name, start_date, end_date, conn)

                    if not rows:
                        logger.info(f"No data returned for heatmap '{metric}', skipping.")
                        continue

                    df = pd.DataFrame(rows)
                    value_field = cfg.get("value_field")

                    if 'variation_id' not in df.columns or 'cohort_day' not in df.columns or value_field not in df.columns:
                        logger.error(
                            f"Heatmap data for metric '{metric}' is missing required columns (variation_id, cohort_day, {value_field}).")
                        continue

                    # 在内存中执行 pivot 操作，生成宽表
                    pivoted_table = df.pivot_table(index='variation_id', columns='cohort_day', values=value_field)
                    result_df = pivoted_table.reset_index()
                    final_json_result = result_df.fillna(0).to_dict(orient='records')

                    # 将最终的JSON结果直接写入查询缓存表
                    write_to_query_cache(
                        conn, 'heatmap', experiment_name, metric, cfg.get("category", ""),
                        start_date, end_date, final_json_result
                    )
                    logger.info(f"Successfully pre-calculated and cached HEATMAP for metric: '{metric}'")

                except Exception as e:
                    logger.error(f"Error processing HEATMAP for metric '{metric}': {e}", exc_info=True)

                # 处理完热力图后，跳过本轮循环的剩余部分
                continue

            # --- 旧逻辑：处理普通趋势图指标 ---
            logger.info(f"Processing as TREND: {metric}")
            try:
                # （此处的翻页逻辑保持不变）
                sig = signature(fetch_func)
                supports_chunk = 'limit' in sig.parameters and 'offset' in sig.parameters
                if supports_chunk:
                    rows = []
                    offset = 0
                    chunk_size = 10000
                    while True:
                        chunk = call_fetch_func_compatible(fetch_func, experiment_name, start_date, end_date, conn,
                                                           limit=chunk_size, offset=offset)
                        if not chunk: break
                        rows.extend(chunk)
                        offset += chunk_size
                else:
                    rows = call_fetch_func_compatible(fetch_func, experiment_name, start_date, end_date, conn)
            except Exception as e:
                logger.error(f"Error fetching data for trend metric '{metric}': {e}", exc_info=True)
                continue

            # (后续的 getval, params_to_insert, INSERT INTO abtest_metric_snapshot 的逻辑完全不变)
            variation_field = cfg.get("variation_field")
            value_field = cfg.get("value_field")
            revenue_field = cfg.get("revenue_field")
            order_field = cfg.get("order_field")
            date_field = cfg.get("date_field")
            category = cfg.get("category", "")
            params_to_insert = []

            for row in rows:
                row_dict = dict(row)  # 转换为字典以便于getval处理
                variation_id = getval(row_dict, variation_field) or "default"
                value = getval(row_dict, value_field)
                revenue = getval(row_dict, revenue_field)
                order = getval(row_dict, order_field)
                event_date = getval(row_dict, date_field)
                if event_date and hasattr(event_date, 'strftime'):
                    event_date_str = event_date.strftime("%Y-%m-%d")
                elif event_date:
                    event_date_str = str(event_date)[:10]
                else:
                    event_date_str = None

                params_to_insert.append((
                    "trend", experiment_name, metric, category, str(variation_id),
                    event_date_str, start_date, end_date,
                    float(value) if value is not None else None,
                    float(revenue) if revenue is not None else None,
                    int(order) if order is not None else None,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    json.dumps(row_dict, default=str)
                ))

            if params_to_insert:
                try:
                    insert_sql = """
                        INSERT INTO abtest_metric_snapshot
                        (query_type, experiment_name, metric, category, variation_id, event_date, start_date, end_date, value, revenue, orders, updated_at, result_json)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    # 注意：根据您的DBAPI驱动，占位符可能是 %s 或 ?
                    conn.execute(text(insert_sql), params_to_insert)
                    logger.info(
                        f"Bulk insert successful for trend metric '{metric}': {len(params_to_insert)} rows inserted.")
                except Exception as err:
                    logger.error(f"Bulk insert failed for trend metric '{metric}': {err}", exc_info=True)

    logger.info("=" * 60)


# main 函数保持不变
def main():
    engine = get_db_connection()
    experiments = get_valid_experiments()
    logger.info(f"Found {len(experiments)} experiments, starting to run all metrics...\n")
    for exp in experiments:
        experiment_name = exp["experiment_name"]
        start_date = exp["phase_start_time"][:10]
        end_date = exp.get("phase_end_time", datetime.now().strftime("%Y-%m-%d"))[:10]

        logger.info(f"==== Running all metrics for: {experiment_name}  ({start_date} ~ {end_date}) ====")
        run_all_metrics(experiment_name, start_date, end_date, engine)


if __name__ == "__main__":
    main()