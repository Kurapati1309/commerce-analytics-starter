from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dag_snapshot_inventory",
    start_date=days_ago(1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["snapshot","hourly"],
) as dag:
    # TODO: read inventory into snapshot table
    step = EmptyOperator(task_id="snapshot")
