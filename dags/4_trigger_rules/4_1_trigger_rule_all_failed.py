"""4_1_trigger_rule_all_failed

Демонстрация TriggerRule.ALL_FAILED.

Сценарий:
- task_a_fail -> падает
- task_b_fail -> падает
- notify_if_all_failed -> сработает ТОЛЬКО если *все* upstream-задачи упали

Удобно видеть поведение в UI: notify_if_all_failed будет SUCCESS, несмотря на падение upstream.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


def _work(msg: str):
    print(msg)


def _fail(msg: str):
    print(msg)
    raise Exception(msg)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


dag = DAG(
    dag_id="4_1_trigger_rule_all_failed",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["trigger_rule", "demo"],
)


task_a_fail = PythonOperator(
    task_id="task_a_fail",
    python_callable=_fail,
    op_args=["Task A failed on purpose"],
    dag=dag,
)

task_b_fail = PythonOperator(
    task_id="task_b_fail",
    python_callable=_fail,
    op_args=["Task B failed on purpose"],
    dag=dag,
)

notify_if_all_failed = PythonOperator(
    task_id="notify_if_all_failed",
    python_callable=_work,
    op_args=["ALL_FAILED fired: both upstream tasks failed"],
    trigger_rule=TriggerRule.ALL_FAILED,
    dag=dag,
)

[task_a_fail, task_b_fail] >> notify_if_all_failed

