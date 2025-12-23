"""4_2_trigger_rule_one_success

Демонстрация TriggerRule.ONE_SUCCESS.

Сценарий:
- task_left_success -> SUCCESS
- task_right_fail -> FAIL
- continue_if_one_success -> сработает, потому что хотя бы один upstream успешен

Хорошо подходит для "частичного успеха", когда допустимо продолжать при наличии результата хотя бы от одной ветки.
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
    dag_id="4_2_trigger_rule_one_success",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["trigger_rule", "demo"],
)


task_left_success = PythonOperator(
    task_id="task_left_success",
    python_callable=_work,
    op_args=["Left path succeeded"],
    dag=dag,
)

task_right_fail = PythonOperator(
    task_id="task_right_fail",
    python_callable=_fail,
    op_args=["Right path failed on purpose"],
    dag=dag,
)

continue_if_one_success = PythonOperator(
    task_id="continue_if_one_success",
    python_callable=_work,
    op_args=["ONE_SUCCESS fired: at least one upstream task succeeded"],
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag,
)

[task_left_success, task_right_fail] >> continue_if_one_success

