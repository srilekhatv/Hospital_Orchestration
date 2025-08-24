from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime

def log_file(execution_date, **kwargs):
    file_date = execution_date.strftime("%Y-%m-%d")
    print(f"Scheduled to load file: {file_date}.csv")

with DAG(
    dag_id="hospital_daily_pipeline",
    start_date=datetime(2025, 8, 24),
    end_date=datetime(2025, 9, 1),
    schedule_interval="@daily",
    catchup=True,
    tags=["snowflake", "dbt", "hospital"]
) as dag:

    # Step 1: Log file name
    log_task = PythonOperator(
        task_id="log_filename",
        python_callable=log_file,
        op_kwargs={"execution_date": "{{ execution_date }}"},
        provide_context=True,
    )

    # Step 2: Load file into Snowflake
    load_task = SnowflakeOperator(
        task_id="load_daily_file",
        snowflake_conn_id="snowflake_conn",
        sql="""
            COPY INTO RAW.PATIENT_VISITS
            FROM @RAW.HOSPITAL_STAGE/{{ execution_date.strftime('%Y-%m-%d') }}.csv
            FILE_FORMAT = (FORMAT_NAME = RAW.CLEANED_CSV_FORMAT)
            ON_ERROR='CONTINUE';
        """
    )

    # Step 3: Row count check
    rowcount_task = SnowflakeOperator(
        task_id="check_row_count",
        snowflake_conn_id="snowflake_conn",
        sql="SELECT COUNT(*) FROM RAW.PATIENT_VISITS;"
    )

    # Step 4: Capture COPY history (rows inserted, errors, duration)
    copy_metrics = SnowflakeOperator(
        task_id="capture_copy_metrics",
        snowflake_conn_id="snowflake_conn",
        sql="""
            SELECT FILE_NAME, ROW_COUNT, ERROR_COUNT, LAST_LOAD_TIME
            FROM INFORMATION_SCHEMA.LOAD_HISTORY
            WHERE TABLE_NAME='PATIENT_VISITS'
            ORDER BY LAST_LOAD_TIME DESC
            LIMIT 1;
        """
    )

    # Step 5: Trigger dbt Run (via dbt Cloud API or Cosmos)
    dbt_run = SimpleHttpOperator(
        task_id="dbt_run",
        http_conn_id="dbt_cloud_conn",
        endpoint="api/v2/accounts/<account_id>/jobs/<job_id>/run/",
        method="POST",
        headers={"Authorization": "Token <dbt_token>"},
    )

    # Step 6: Trigger dbt Test
    dbt_test = SimpleHttpOperator(
        task_id="dbt_test",
        http_conn_id="dbt_cloud_conn",
        endpoint="api/v2/accounts/<account_id>/jobs/<job_id>/run/",
        method="POST",
        headers={"Authorization": "Token <dbt_token>"},
        data='{"cause":"Triggered from Airflow: dbt test"}',
    )

    log_task >> load_task >> rowcount_task >> copy_metrics >> dbt_run >> dbt_test
