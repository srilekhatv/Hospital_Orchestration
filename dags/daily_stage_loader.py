from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

with DAG(
    "snowflake_test",
    start_date=datetime(2025, 8, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    test_task = SnowflakeOperator(
        task_id="check_row_count",
        snowflake_conn_id="snowflake_conn",
        sql="SELECT COUNT(*) FROM RAW.PATIENT_VISITS;"
    )
