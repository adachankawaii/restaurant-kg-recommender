from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="weekly_rgcn_training",
    start_date=datetime(2026, 6, 1),
    schedule="0 4 * * 0",
    catchup=False,
    tags=["restaurant", "rgcn"],
) as dag:
    build_training_set = BashOperator(
        task_id="build_training_set_from_clicked_scenarios",
        bash_command="cd /opt/restaurant-kg-recommender/production && python scripts/build_weekly_rgcn_training_set.py",
    )

    export_snapshot = BashOperator(
        task_id="export_rgcn_snapshot",
        bash_command="cd /opt/restaurant-kg-recommender/production && python scripts/export_rgcn_snapshot.py --mode online",
    )

    train_rgcn = BashOperator(
        task_id="train_rgcn",
        bash_command="cd /opt/restaurant-kg-recommender/production && python scripts/train_rgcn.py --mode online",
    )

    build_training_set >> export_snapshot >> train_rgcn
