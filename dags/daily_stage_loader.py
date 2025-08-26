from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable
from datetime import datetime
import json

# Config
ACCOUNT_ID = "70471823487465"   # your dbt account id
RUN_JOB_ID = Variable.get("DBT_RUN_JOB_ID")   # Prod Run Job ID from Airflow Variables
TEST_JOB_ID = Variable.get("DBT_TEST_JOB_ID") # Prod Test Job ID from Airflow Variables
DBT_SERVICE_TOKEN = "dbtu_uwGba_sY2eqd6NcQLh8u_6pdFHgk9ezQcjvvDHmgYb81xNim8I"  # your service token


# Step 0: Log file for debugging
def log_file(execution_date, **kwargs):
    print(f"Scheduled to load file for execution date: {execution_date}")

# Step 4: Data quality check
def validate_rows(**context):
    rows = context['ti'].xcom_pull(task_ids='check_row_count')
    if rows:
        count = list(rows[0].values())[0]  # SnowflakeOperator returns [{'COUNT(*)': value}]
        if int(count) == 0:
            raise AirflowFailException("❌ No rows loaded into RAW.PATIENT_VISITS")
        print(f"✅ Validation passed: {count} total rows in RAW")
    else:
        raise AirflowFailException("❌ No result returned from row count query")


with DAG(
    dag_id="daily_stage_loader_http",
    start_date=datetime(2025, 8, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["snowflake", "hospital", "dbt"]
) as dag:

    # Step 1: Truncate RAW
    truncate_task = SnowflakeOperator(
        task_id="truncate_table",
        snowflake_conn_id="snowflake_conn",
        sql="TRUNCATE TABLE IF EXISTS RAW.PATIENT_VISITS;"
    )

    # Step 2: Log file name
    log_task = PythonOperator(
        task_id="log_filename",
        python_callable=log_file,
    )

    # Step 3: Load file into RAW
    load_task = SnowflakeOperator(
        task_id="load_daily_file",
        snowflake_conn_id="snowflake_conn",
        sql="""
            COPY INTO RAW.PATIENT_VISITS
            FROM @RAW.HOSPITAL_STAGE/{{ (execution_date + macros.timedelta(days=1)).strftime('%Y-%m-%d') }}.csv
            FILE_FORMAT = (FORMAT_NAME = RAW.CLEANED_CSV_FORMAT)
            ON_ERROR = 'CONTINUE';
        """
    )

    # Step 4: Row count check
    rowcount_task = SnowflakeOperator(
        task_id="check_row_count",
        snowflake_conn_id="snowflake_conn",
        sql="SELECT COUNT(*) FROM RAW.PATIENT_VISITS;",
        do_xcom_push=True,
    )

    # Step 5: Validate load
    validate_task = PythonOperator(
        task_id="validate_load",
        python_callable=validate_rows,
    )

    # Step 6: Insert audit log
    audit_log_task = SnowflakeOperator(
        task_id="insert_audit_log",
        snowflake_conn_id="snowflake_conn",
        sql="""
            INSERT INTO AUDIT.LOAD_LOGS (execution_date, source_file, rows_loaded)
            SELECT 
                '{{ ds }}'::DATE AS execution_date,
                METADATA$FILENAME AS source_file,
                COUNT(*) AS rows_loaded
            FROM @RAW.HOSPITAL_STAGE/{{ (execution_date + macros.timedelta(days=1)).strftime('%Y-%m-%d') }}.csv
            (FILE_FORMAT => RAW.CLEANED_CSV_FORMAT)
            GROUP BY METADATA$FILENAME;
        """
    )

    # Step 7: Trigger dbt run (via API)
    dbt_run = SimpleHttpOperator(
        task_id="dbt_run",
        http_conn_id="dbt_http_conn",   # defined in Astronomer
        endpoint=f"api/v2/accounts/{ACCOUNT_ID}/jobs/{RUN_JOB_ID}/run/",
        method="POST",
        headers={
            "Authorization": f"Token {DBT_SERVICE_TOKEN}",
            "Content-Type": "application/json"
        },
        data=json.dumps({"cause": "Triggered by Airflow - Run Job"})
    )

    # Step 8: Trigger dbt test (via API)
    dbt_test = SimpleHttpOperator(
        task_id="dbt_test",
        http_conn_id="dbt_http_conn",
        endpoint=f"api/v2/accounts/{ACCOUNT_ID}/jobs/{TEST_JOB_ID}/run/",
        method="POST",
        headers={
            "Authorization": f"Token {DBT_SERVICE_TOKEN}",
            "Content-Type": "application/json"
        },
        data=json.dumps({"cause": "Triggered by Airflow - Test Job"})
    )

    # DAG flow
    truncate_task >> log_task >> load_task >> rowcount_task >> validate_task >> audit_log_task >> dbt_run >> dbt_test
