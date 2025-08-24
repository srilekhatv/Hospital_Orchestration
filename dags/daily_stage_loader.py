from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

with DAG(
    dag_id="daily_stage_loader",
    start_date=datetime(2025, 8, 24),   # first file date
    end_date=datetime(2025, 9, 1),      # last file date
    schedule_interval="@daily",         # run once per day at midnight
    catchup=True,                       # backfill daily runs if needed
    tags=["snowflake", "hospital"]
) as dag:

    load_task = SnowflakeOperator(
        task_id="load_daily_file",
        snowflake_conn_id="snowflake_conn",  # Airflow Connection ID you created
        sql="""
            COPY INTO RAW.PATIENT_VISITS
            FROM @RAW.HOSPITAL_STAGE/{{ execution_date.strftime('%Y-%m-%d') }}.csv
            FILE_FORMAT = (FORMAT_NAME = RAW.CLEANED_CSV_FORMAT)
            ON_ERROR='CONTINUE';
        """
    )
