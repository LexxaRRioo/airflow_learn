"""
Пример DAG для демонстрации `schedule_interval` с пресетом `@daily`.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from airflow.decorators import dag, task

@dag(
    dag_id="3_2_start_date_schedule_preset",
    description="Пример start_date с preset schedule (@daily)",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["tutorial", "schedule", "preset"],
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
)
def daily_preset_dag():
    @task
    def task_one():
        """Первая простая задача."""
        print("Эта задача запускается ежедневно по пресету @daily.")

    @task
    def task_two(input_from_task_one: str):
        """Вторая простая задача, получающая данные от первой."""
        print(f"Вторая задача получила: '{input_from_task_one}'")

    # Установка зависимостей
    result = task_one()
    task_two(result)

# Инстанцирование DAG
daily_preset_dag()

