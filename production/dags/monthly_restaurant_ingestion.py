from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="monthly_restaurant_ingestion",
    start_date=datetime(2026, 6, 1),
    schedule="0 2 1 * *",
    catchup=False,
    tags=["restaurant", "ingestion"],
) as dag:
    monthly_ingest = BashOperator(
        task_id="crawl_dump_normalize_build_sources",
        bash_command="cd /opt/restaurant-kg-recommender/production && RUN_FOODY_CRAWLER=true python scripts/run_monthly_restaurant_ingestion.py",
    )
