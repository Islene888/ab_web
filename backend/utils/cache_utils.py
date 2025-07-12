import json
from datetime import datetime
from sqlalchemy import text

CACHE_EXPIRE_HOURS = 6

def get_abtest_cache(
    engine_local, query_type, experiment_name, metric, category, start_date, end_date
):
    sql = text("""
        SELECT result_json, updated_at
        FROM abtest_query_cache
        WHERE query_type=:query_type
          AND experiment_name=:experiment_name
          AND metric=:metric
          AND category=:category
          AND start_date=:start_date
          AND end_date=:end_date
    """)
    with engine_local.connect() as conn:
        row = conn.execute(
            sql,
            {
                "query_type": query_type,
                "experiment_name": experiment_name,
                "metric": metric,
                "category": category,
                "start_date": start_date,
                "end_date": end_date,
            }
        ).fetchone()
        if row:
            updated_at = row[1]
            if isinstance(updated_at, str):
                updated_at = datetime.fromisoformat(updated_at)
            if (datetime.now() - updated_at).total_seconds() < CACHE_EXPIRE_HOURS * 3600:
                return json.loads(row[0])
    return None

def set_abtest_cache(
    engine_local, query_type, experiment_name, metric, category, start_date, end_date, result_json
):
    sql = text("""
        INSERT INTO abtest_query_cache
        (query_type, experiment_name, metric, category, start_date, end_date, result_json, updated_at)
        VALUES (:query_type, :experiment_name, :metric, :category, :start_date, :end_date, :result_json, NOW())
        ON DUPLICATE KEY UPDATE result_json=VALUES(result_json), updated_at=NOW()
    """)
    with engine_local.begin() as conn:
        conn.execute(
            sql,
            {
                "query_type": query_type,
                "experiment_name": experiment_name,
                "metric": metric,
                "category": category,
                "start_date": start_date,
                "end_date": end_date,
                "result_json": json.dumps(result_json, ensure_ascii=False),
            }
        )
