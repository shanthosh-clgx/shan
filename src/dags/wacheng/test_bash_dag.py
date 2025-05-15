import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)
default_args = {
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    "wacheng_test_bash_dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 3, 17),
    catchup=False,
    tags=["wacheng", "challenge_0"]
) as dag:
    ls_dir = BashOperator(
        task_id="ls_dir",
        bash_command="ls -la",
        do_xcom_push=True,
    )

    ls_dir
    