from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def log_file(execution_date, **kwargs):
    print(f"Scheduled to load file for execution date: {execution_date}")

with DAG(
    dag_id="daily_stage_loader",
    start_date=datetime(2025, 8, 24),
    end_date=datetime(2025, 9, 1),
    schedule_interval="@daily",
    catchup=True,
    tags=["snowflake", "hospital"]
) as dag:

    # Step 1: Log file name
    log_task = PythonOperator(
        task_id="log_filename",
        python_callable=log_file,
    )

    # Step 2: Load file into Snowflake
    load_task = SnowflakeOperator(
        task_id="load_daily_file",
        snowflake_conn_id="snowflake_conn",
        sql="""
            COPY INTO RAW.PATIENT_VISITS
            FROM @RAW.HOSPITAL_STAGE/{{ ds }}.csv
            FILE_FORMAT = (FORMAT_NAME = RAW.CLEANED_CSV_FORMAT)
            ON_ERROR='CONTINUE';
        """
    )

    # Step 3: Row count check
    rowcount_task = SnowflakeOperator(
        task_id="check_row_count",
        snowflake_conn_id="snowflake_conn",
        sql="SELECT COUNT(*) FROM RAW.PATIENT_VISITS;",
    )

    log_task >> load_task >> rowcount_task
