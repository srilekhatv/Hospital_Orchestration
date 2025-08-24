from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime


def log_file(execution_date, **kwargs):
    print(f"Scheduled to load file for execution date: {execution_date}")


# ✅ Data quality check
def validate_rows(**context):
    rows = context['ti'].xcom_pull(task_ids='check_row_count')
    if rows and int(rows[0][0]) == 0:
        raise AirflowFailException("❌ No rows loaded into RAW.PATIENT_VISITS")
    print(f"✅ Validation passed: {rows[0][0]} total rows in RAW")


with DAG(
    dag_id="daily_stage_loader",
    start_date=datetime(2025, 8, 24),
    end_date=datetime(2025, 9, 1),
    schedule_interval="@daily",     # run every midnight
    catchup=True,                   # simulate backfill
    tags=["snowflake", "hospital", "dbt"]
) as dag:

    # Step 0: Truncate RAW (only first run; comment later if not needed)
    truncate_task = SnowflakeOperator(
        task_id="truncate_table",
        snowflake_conn_id="snowflake_conn",
        sql="TRUNCATE TABLE IF EXISTS RAW.PATIENT_VISITS;"
    )

    # Step 1: Log file name
    log_task = PythonOperator(
        task_id="log_filename",
        python_callable=log_file,
    )

    # Step 2: Load file into RAW with source_file
    load_task = SnowflakeOperator(
        task_id="load_daily_file",
        snowflake_conn_id="snowflake_conn",
        sql="""
            COPY INTO RAW.PATIENT_VISITS
            FROM (
                SELECT *, METADATA$FILENAME AS source_file
                FROM @RAW.HOSPITAL_STAGE/{{ ds }}.csv t
            )
            FILE_FORMAT = (FORMAT_NAME = RAW.CLEANED_CSV_FORMAT)
            ON_ERROR = 'CONTINUE';
        """
    )

    # Step 3: Row count check
    rowcount_task = SnowflakeOperator(
        task_id="check_row_count",
        snowflake_conn_id="snowflake_conn",
        sql="SELECT COUNT(*) FROM RAW.PATIENT_VISITS;",
    )

    # Step 4: Validate load (fail if 0 rows)
    validate_task = PythonOperator(
        task_id="validate_load",
        python_callable=validate_rows,
        provide_context=True,
    )

    # Step 5: Insert audit log
    audit_log_task = SnowflakeOperator(
        task_id="insert_audit_log",
        snowflake_conn_id="snowflake_conn",
        sql="""
            INSERT INTO AUDIT.LOAD_LOGS (execution_date, source_file, rows_loaded)
            SELECT 
                '{{ ds }}'::DATE AS execution_date,
                source_file,
                COUNT(*) AS rows_loaded
            FROM RAW.PATIENT_VISITS
            WHERE source_file LIKE '%{{ ds }}%'
            GROUP BY source_file;
        """
    )

    # Step 6: dbt run
    dbt_run = DbtCloudRunJobOperator(
        task_id="dbt_run",
        job_id=70471823500217,        # Hospital Run Job ID
        dbt_cloud_conn_id="dbt_cloud_conn",
        check_interval=30,
        timeout=600,
    )

    # Step 7: dbt test
    dbt_test = DbtCloudRunJobOperator(
        task_id="dbt_test",
        job_id=70471823500220,        # Hospital Test Job ID
        dbt_cloud_conn_id="dbt_cloud_conn",
        check_interval=30,
        timeout=600,
    )

    # DAG flow
    truncate_task >> log_task >> load_task >> rowcount_task >> validate_task >> audit_log_task >> dbt_run >> dbt_test
