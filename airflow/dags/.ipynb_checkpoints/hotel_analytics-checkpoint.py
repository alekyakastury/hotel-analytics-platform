from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {"owner": "alekya", "retries": 0}

with DAG(
    dag_id="hotel_analytics_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule=None,  # manual trigger (best for demo)
    catchup=False,
    tags=["hotel", "portfolio"],
) as dag:

    # 1) Run your Python loader (Postgres -> Snowflake)
    load_to_snowflake = BashOperator(
        task_id="load_postgres_to_snowflake",
        bash_command="""
        set -e
        cd /opt/airflow
        python /opt/airflow/scripts/postgres_to_snowflake.py
        """,
    )

    # 2) Run dbt
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        set -e
        cd /opt/airflow/dbt/hotel_analytics
        dbt deps
        dbt run
        """,
    )

    # Optional: tests (keep off if flaky)
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
        set -e
        cd /opt/airflow/dbt/hotel_analytics
        dbt test
        """,
    )

    load_to_snowflake >> dbt_run >> dbt_test
