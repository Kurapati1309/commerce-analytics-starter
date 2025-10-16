from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dag_dbt_transform",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["dbt","daily"],
) as dag:

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd /opt/airflow/dags/dbt && dbt build --fail-fast"
    )
