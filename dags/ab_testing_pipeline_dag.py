# dags/ab_testing_pipeline_dag.py

from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator

# 因为 backend 文件夹被完整挂载到了 /opt/airflow/backend，
# 并且 Airflow 会自动将 /opt/airflow/ 加入 PYTHONPATH，
# 所以可以直接从 backend 开始导入。
from backend.airflow.run_all_metrics import main as snapshot_main
from backend.airflow.summary_cache import main as aggregate_main


@dag(
    dag_id='ab_testing_pipeline',
    default_args={
        'owner': 'ella',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='每日ETL流程：生成指标快照，然后聚合结果到缓存',
    schedule_interval='@daily',
    start_date=datetime(2024, 8, 1),
    catchup=False,
    tags=['ab-testing', 'etl'],
)
def ab_testing_pipeline():
    """
    This DAG runs the daily snapshot and aggregation tasks for A/B testing.
    """

    task_generate_snapshot = PythonOperator(
        task_id='generate_metric_snapshots',
        python_callable=snapshot_main,
    )

    task_aggregate_and_cache = PythonOperator(
        task_id='aggregate_and_cache_results',
        python_callable=aggregate_main,
    )

    task_generate_snapshot >> task_aggregate_and_cache


# 实例化 DAG
ab_testing_pipeline()