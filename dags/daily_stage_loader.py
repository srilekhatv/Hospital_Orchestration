from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime


def log_file(execution_date, **kwargs):
    print(f"Scheduled to load file for execution date: {execution_date}")


# ✅ Step 1: Data validation function
def validate_rows(**context):
    # pull row count from previous task
    rows = context['ti'].xcom_pull(task_ids='check_row_count')
    if rows and int(rows[0][0]) == 0:
        raise AirflowFailException("No rows loaded into RAW.PATIENT_VISITS")
    print(f"Validation passed: {rows[0][0]} total rows in RAW")


with DAG(
    dag_id="daily_stage_loader",
    start_date=datetime(2025, 8, 24),
    end_date=datetime(2025, 9, 1),
    schedule_interval="@daily",
    catchup=True,
    tags=["snowflake", "hospital", "dbt"]
) as dag:

    truncate_task = SnowflakeOperator(
        task_id="truncate_table",
        snowflake_conn_id="snowflake_conn",
        sql="TRUNCATE TABLE IF EXISTS RAW.PATIENT_VISITS;"
    )

    log_task = PythonOperator(
        task_id="log_filename",
        python_callable=log_file,
    )

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

    rowcount_task = SnowflakeOperator(
        task_id="check_row_count",
        snowflake_conn_id="snowflake_conn",
        sql="SELECT COUNT(*) FROM RAW.PATIENT_VISITS;",
    )

    # Step 1: Data validation operator
    validate_task = PythonOperator(
        task_id="validate_load",
        python_callable=validate_rows,
        provide_context=True,
    )

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

    dbt_run = DbtCloudRunJobOperator(
        task_id="dbt_run",
        job_id=70471823500217,
        dbt_cloud_conn_id="dbt_cloud_conn",
        check_interval=30,
        timeout=600,
    )

    dbt_test = DbtCloudRunJobOperator(
        task_id="dbt_test",
        job_id=70471823500220,
        dbt_cloud_conn_id="dbt_cloud_conn",
        check_interval=30,
        timeout=600,
    )

    # Step 2: Slack alert if any task fails
    slack_alert = SlackWebhookOperator(
        task_id="slack_alert",
        http_conn_id="slack_conn_id",   # configure in Airflow Connections
        message="❌ DAG {{ dag.dag_id }} failed on {{ ds }}",
        trigger_rule="one_failed"       # run only if something upstream fails
    )

    # DAG flow
    truncate_task >> log_task >> load_task >> rowcount_task >> validate_task >> audit_log_task >> dbt_run >> dbt_test >> slack_alert
