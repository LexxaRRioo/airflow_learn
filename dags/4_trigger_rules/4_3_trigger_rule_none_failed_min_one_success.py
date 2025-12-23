"""4_3_trigger_rule_none_failed_min_one_success

Демонстрация TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS.

Сценарий:
- task_a_success -> SUCCESS
- task_b_skipped  -> SKIPPED (мы явно поднимаем AirflowSkipException)
- join -> сработает, потому что:
    - нет FAILED
    - есть хотя бы один SUCCESS

Это типичный trigger_rule для "join" после ветвления (branching).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


def _work(msg: str):
    print(msg)


def _skip(msg: str):
    print(msg)
    raise AirflowSkipException(msg)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


dag = DAG(
    dag_id="4_3_trigger_rule_none_failed_min_one_success",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["trigger_rule", "demo"],
)


task_a_success = PythonOperator(
    task_id="task_a_success",
    python_callable=_work,
    op_args=["Task A succeeded"],
    dag=dag,
)

task_b_skipped = PythonOperator(
    task_id="task_b_skipped",
    python_callable=_skip,
    op_args=["Task B skipped on purpose"],
    dag=dag,
)

join = PythonOperator(
    task_id="join",
    python_callable=_work,
    op_args=["NONE_FAILED_MIN_ONE_SUCCESS fired: no failures and at least one success"],
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)

[task_a_success, task_b_skipped] >> join

