from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dag_ingest_batch_daily",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["ingest","daily"],
) as dag:
    start = EmptyOperator(task_id="start")
    # TODO: sensors for S3 keys and COPY into Redshift staging
    finish = EmptyOperator(task_id="finish")
    start >> finish
