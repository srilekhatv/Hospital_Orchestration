from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from datetime import datetime


# Python logging function
def log_file(execution_date, **kwargs):
    print(f"Scheduled to load file for execution date: {execution_date}")


with DAG(
    dag_id="daily_stage_loader",
    start_date=datetime(2025, 8, 24),
    end_date=datetime(2025, 9, 1),
    schedule_interval="@daily",     # run every midnight
    catchup=True,                   # allow backfills
    tags=["snowflake", "hospital", "dbt"]
) as dag:

    # Step 0: Truncate RAW table (first run only, then disable/comment out)
    truncate_task = SnowflakeOperator(
        task_id="truncate_table",
        snowflake_conn_id="snowflake_conn",
        sql="TRUNCATE TABLE IF EXISTS RAW.PATIENT_VISITS;"
    )

    # Step 1: Log which file is being processed
    log_task = PythonOperator(
        task_id="log_filename",
        python_callable=log_file,
    )

    # Step 2: Load chunk file into RAW (capture source_file)
    load_task = SnowflakeOperator(
        task_id="load_daily_file",
        snowflake_conn_id="snowflake_conn",
        sql="""
            COPY INTO RAW.PATIENT_VISITS
            FROM (
                SELECT
                    t.$1 AS encounter_id,
                    t.$2 AS patient_nbr,
                    t.$3 AS race,
                    t.$4 AS gender,
                    t.$5 AS age,
                    t.$6 AS admission_type_id,
                    t.$7 AS discharge_disposition_id,
                    t.$8 AS admission_source_id,
                    t.$9 AS time_in_hospital,
                    t.$10 AS payer_code,
                    t.$11 AS medical_specialty,
                    t.$12 AS num_lab_procedures,
                    t.$13 AS num_procedures,
                    t.$14 AS num_medications,
                    t.$15 AS number_outpatient,
                    t.$16 AS number_emergency,
                    t.$17 AS number_inpatient,
                    t.$18 AS diag_1,
                    t.$19 AS diag_2,
                    t.$20 AS diag_3,
                    t.$21 AS number_diagnoses,
                    t.$22 AS max_glu_serum,
                    t.$23 AS A1Cresult,
                    t.$24 AS metformin,
                    t.$25 AS repaglinide,
                    t.$26 AS nateglinide,
                    t.$27 AS chlorpropamide,
                    t.$28 AS glimepiride,
                    t.$29 AS acetohexamide,
                    t.$30 AS glipizide,
                    t.$31 AS glyburide,
                    t.$32 AS tolbutamide,
                    t.$33 AS pioglitazone,
                    t.$34 AS rosiglitazone,
                    t.$35 AS acarbose,
                    t.$36 AS miglitol,
                    t.$37 AS troglitazone,
                    t.$38 AS tolazamide,
                    t.$39 AS examide,
                    t.$40 AS citoglipton,
                    t.$41 AS insulin,
                    t.$42 AS glyburide_metformin,
                    t.$43 AS glipizide_metformin,
                    t.$44 AS glimepiride_pioglitazone,
                    t.$45 AS metformin_rosiglitazone,
                    t.$46 AS metformin_pioglitazone,
                    t.$47 AS change,
                    t.$48 AS diabetesMed,
                    t.$49 AS readmitted,
                    t.$50 AS readmitted_flag,
                    METADATA$FILENAME AS source_file
                FROM @RAW.HOSPITAL_STAGE/{{ ds }}.csv t
            )
            FILE_FORMAT = (FORMAT_NAME = RAW.CLEANED_CSV_FORMAT)
            ON_ERROR = 'CONTINUE';
        """
    )

    # Step 3: Row count check after load
    rowcount_task = SnowflakeOperator(
        task_id="check_row_count",
        snowflake_conn_id="snowflake_conn",
        sql="SELECT COUNT(*) AS total_rows FROM RAW.PATIENT_VISITS;",
    )

    # Step 4: Run dbt models
    dbt_run = DbtCloudRunJobOperator(
        task_id="dbt_run",
        job_id=<YOUR_DBT_CLOUD_RUN_JOB_ID>,   # replace with your dbt Cloud job for `dbt run`
        check_interval=30,
        timeout=600,
    )

    # Step 5: Test dbt models
    dbt_test = DbtCloudRunJobOperator(
        task_id="dbt_test",
        job_id=<YOUR_DBT_CLOUD_TEST_JOB_ID>,  # replace with your dbt Cloud job for `dbt test`
        check_interval=30,
        timeout=600,
    )

    # DAG flow
    truncate_task >> log_task >> load_task >> rowcount_task >> dbt_run >> dbt_test
