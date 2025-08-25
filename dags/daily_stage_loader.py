from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime

# Replace with your values
ACCOUNT_ID = "70471823487465"   # from your dbt URL
JOB_ID = "70471823500217"       # your dbt job id

with DAG(
    dag_id="dbt_trigger_with_service_token",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt", "http"]
) as dag:

    # Task: Trigger dbt job run
    trigger_dbt = SimpleHttpOperator(
        task_id="trigger_dbt",
        http_conn_id="dbt_http_conn",   # configure this in Astronomer
        endpoint=f"api/v2/accounts/{ACCOUNT_ID}/jobs/{JOB_ID}/run/",
        method="POST",
        headers={
            "Authorization": "Token dbtu_XXXXXXXXXXXXXXXXXX",  # your service token
            "Content-Type": "application/json"
        },
        data="{}"  # empty JSON payload triggers job with default settings
    )
