from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.standard.operators.bash import BashOperator

SPARK_CONTAINER = "nyc-taxi-spark"
SQL_DIR = "/opt/airflow/src/SQL/silver"
POSTGRES_CONN_ID = "postgres_default"

default_args = {
    "owner": "TurboBlender",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="nyc_taxi_lakehouse",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="*/30 * * * *",
    catchup=False,
    template_searchpath=[SQL_DIR],
    tags=["nyc-taxi", "lakehouse"],
) as dag:

    wait_for_postgres = SqlSensor(
        task_id="wait_for_postgres",
        conn_id=POSTGRES_CONN_ID,
        sql="SELECT 1;",
        poke_interval=30,
        timeout=600,
        mode="reschedule",
    )

    create_raw_schema = SQLExecuteQueryOperator(
        task_id="create_raw_schema",
        conn_id=POSTGRES_CONN_ID,
        sql="CREATE SCHEMA IF NOT EXISTS raw;",
    )

    spark_extract = BashOperator(
        task_id="spark_extract",
        bash_command=(
            f"docker exec {SPARK_CONTAINER} "
            "python3 /app/src/etl/extract.py"
        ),
    )

    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command=(
            f"docker exec {SPARK_CONTAINER} "
            "python3 /app/src/etl/transform.py"
        ),
    )

    spark_load = BashOperator(
        task_id="spark_load",
        bash_command=(
            f"docker exec {SPARK_CONTAINER} "
            "python3 /app/src/etl/load.py"
        ),
    )

    silver_rename = SQLExecuteQueryOperator(
        task_id="silver_01_rename_columns",
        conn_id=POSTGRES_CONN_ID,
        sql="01_rename_columns.sql",
        split_statements=True,
    )

    silver_clean = SQLExecuteQueryOperator(
        task_id="silver_02_clean_records",
        conn_id=POSTGRES_CONN_ID,
        sql="02_clean_records.sql",
        split_statements=True,
    )

    silver_dedup = SQLExecuteQueryOperator(
        task_id="silver_03_deduplicate",
        conn_id=POSTGRES_CONN_ID,
        sql="03_deduplicate.sql",
        split_statements=True,
    )

    silver_derived = SQLExecuteQueryOperator(
        task_id="silver_04_derived_columns",
        conn_id=POSTGRES_CONN_ID,
        sql="04_derived_columns.sql",
        split_statements=True,
    )

    (
        wait_for_postgres
        >> create_raw_schema
        >> spark_extract
        >> spark_transform
        >> spark_load
        >> silver_rename
        >> silver_clean
        >> silver_dedup
        >> silver_derived
    )
