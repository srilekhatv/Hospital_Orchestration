from airflow import DAG
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from datetime import datetime

with DAG(
    "dbt_test",
    start_date=datetime(2025, 8, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    dbt_run = DbtCloudRunJobOperator(
        task_id="dbt_run",
        job_id=70471823500217,   # your dbt job id
        dbt_cloud_conn_id="dbt_cloud_conn"
    )
