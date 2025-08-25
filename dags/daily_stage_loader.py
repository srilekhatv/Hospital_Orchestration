from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime
import json

ACCOUNT_ID = "70471823487465"
JOB_ID = "70471823500217"

with DAG(
    dag_id="dbt_trigger_with_service_token",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt", "http"]
) as dag:

    trigger_dbt = SimpleHttpOperator(
        task_id="trigger_dbt",
        http_conn_id="dbt_http_conn",
        endpoint=f"api/v2/accounts/{ACCOUNT_ID}/jobs/{JOB_ID}/run/",
        method="POST",
        headers={
            "Authorization": "Token dbtu_uwGba_sY2eqd6NcQLh8u_6pdFHgk9ezQcjvvDHmgYb81xNim8I",  # your service token
            "Content-Type": "application/json"
        },
        data=json.dumps({
            "cause": "Triggered by Airflow via Astronomer"
        })
    )
