"""4_4_trigger_rule_all_done

Демонстрация TriggerRule.ALL_DONE.

Сценарий:
- task_prepare -> SUCCESS
- task_may_fail -> FAIL
- cleanup_always -> сработает ВСЕГДА после upstream, даже если были ошибки

Классический сценарий: cleanup/rollback/уведомления, которые должны сработать независимо от результата.
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
    dag_id="4_4_trigger_rule_all_done",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["trigger_rule", "demo"],
)


task_prepare = PythonOperator(
    task_id="task_prepare",
    python_callable=_work,
    op_args=["Preparing resources..."],
    dag=dag,
)

task_may_fail = PythonOperator(
    task_id="task_may_fail",
    python_callable=_fail,
    op_args=["Processing failed on purpose"],
    dag=dag,
)

cleanup_always = PythonOperator(
    task_id="cleanup_always",
    python_callable=_work,
    op_args=["ALL_DONE fired: cleanup executed regardless of upstream states"],
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# 3 таски: prepare -> may_fail -> cleanup_always
# Чтобы видеть ALL_DONE, cleanup зависит от may_fail.
task_prepare >> task_may_fail >> cleanup_always

