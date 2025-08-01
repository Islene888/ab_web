import logging
import json
from collections import defaultdict
from datetime import datetime
from inspect import signature

from backend.service.config import INDICATOR_CONFIG
from backend.utils.engine_utils import get_db_connection
from backend.airflow.experiment_filter import get_valid_experiments
from sqlalchemy.sql import text

logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

def getval(r, field):
    if field is None:
        return None
    if isinstance(field, int):
        try:
            return r[field]
        except (IndexError, TypeError):
            return None
    elif isinstance(field, str) and isinstance(r, dict):
        return r.get(field)
    return None

def call_fetch_func_compatible(fetch_func, experiment_name, start_date, end_date, engine_or_conn, *args, **kwargs):
    if hasattr(engine_or_conn, "connect") and callable(engine_or_conn.connect):
        return fetch_func(experiment_name, start_date, end_date, engine_or_conn, *args, **kwargs)
    else:
        class FakeEngine:
            def connect(self_inner):
                return engine_or_conn
        return fetch_func(experiment_name, start_date, end_date, FakeEngine(), *args, **kwargs)


def run_all_metrics(experiment_name, start_date, end_date, engine=None):
    if engine is None:
        engine = get_db_connection()
    results = {}
    chunk_size = 10000
    with engine.begin() as conn:
        for metric, cfg in INDICATOR_CONFIG.items():
            logger.info("=" * 60)
            logger.info(f"Running metric: {metric}  |  {cfg.get('category', '')}")
            fetch_func = cfg["fetch_func"]

            # Check if fetch_func supports pagination
            try:
                sig = signature(fetch_func)
                supports_chunk = 'limit' in sig.parameters and 'offset' in sig.parameters
            except Exception:
                supports_chunk = False

            # Fetch rows with or without pagination
            try:
                if supports_chunk:
                    rows = []
                    offset = 0
                    while True:
                        try:
                            chunk = call_fetch_func_compatible(
                                fetch_func,
                                experiment_name,
                                start_date,
                                end_date,
                                conn,
                                limit=chunk_size,
                                offset=offset,
                            )
                        except TypeError:
                            # Fallback to non-paginated call
                            rows = call_fetch_func_compatible(
                                fetch_func,
                                experiment_name,
                                start_date,
                                end_date,
                                conn,
                            ) or []
                            break
                        if not chunk:
                            break
                        rows.extend(chunk)
                        offset += chunk_size
                else:
                    rows = call_fetch_func_compatible(
                        fetch_func,
                        experiment_name,
                        start_date,
                        end_date,
                        conn,
                    )
            except Exception as e:
                logger.error(f"Error fetching data for metric {metric}: {e}", exc_info=True)
                results[metric] = {"error": str(e)}
                continue

            variation_field = cfg.get("variation_field")
            value_field = cfg.get("value_field")
            revenue_field = cfg.get("revenue_field")
            order_field = cfg.get("order_field")
            date_field = cfg.get("date_field")
            category = cfg.get("category", "")

            params_to_insert = []
            metric_result = defaultdict(list)

            for row in rows:
                variation_id = getval(row, variation_field) or "default"
                value = getval(row, value_field)
                revenue = getval(row, revenue_field)
                order = getval(row, order_field)
                event_date = getval(row, date_field)

                if event_date and hasattr(event_date, 'strftime'):
                    event_date_str = event_date.strftime("%Y-%m-%d")
                elif event_date:
                    event_date_str = str(event_date)[:10]
                else:
                    event_date_str = None

                metric_result[variation_id].append({
                    "date": event_date_str, "value": value, "revenue": revenue, "order": order
                })

                params_to_insert.append((
                    "trend",
                    experiment_name,
                    metric,
                    category,
                    str(variation_id),
                    event_date_str,
                    start_date,
                    end_date,
                    float(value) if value is not None else None,
                    float(revenue) if revenue is not None else None,
                    int(order) if order is not None else None,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    json.dumps(dict(row) if isinstance(row, dict) else row, default=str)
                ))

            if params_to_insert:
                try:
                    insert_sql = """
                        INSERT INTO abtest_metric_snapshot
                        (query_type, experiment_name, metric, category, variation_id, event_date, start_date, end_date, value, revenue, orders, updated_at, result_json)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    conn.execute(insert_sql, params_to_insert)
                    logger.info(f"Bulk insert successful: metric={metric}, {len(params_to_insert)} rows inserted.")
                except Exception as err:
                    logger.error(f"Bulk insert failed: metric={metric}, Reason: {err}", exc_info=True)

            logger.info(
                f"Metric [{metric}] returned {sum(len(v) for v in metric_result.values())} rows, {len(metric_result)} variations:")
            for variation_id, records in metric_result.items():
                logger.info(f"  Variation: {variation_id} | {len(records)} rows")

            results[metric] = metric_result

    logger.info("=" * 60)
    return results


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
