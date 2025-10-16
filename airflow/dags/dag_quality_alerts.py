from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dag_quality_alerts",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["alerts"],
) as dag:
    # TODO: parse dbt artifacts and send notifications
    noop = EmptyOperator(task_id="noop")
