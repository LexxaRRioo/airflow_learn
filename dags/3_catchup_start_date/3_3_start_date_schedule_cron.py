"""
Пример DAG для демонстрации `schedule_interval` с cron-выражением.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from airflow.decorators import dag, task

@dag(
    dag_id="3_3_start_date_schedule_cron",
    description="Пример start_date с cron schedule (ежедневно в полночь)",
    schedule_interval="0 0 * * *",  # Эквивалент @daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["tutorial", "schedule", "cron"],
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
)
def daily_cron_dag():
    @task
    def task_one():
        """Первая простая задача."""
        print("Эта задача запускается ежедневно по cron-выражению '0 0 * * *'.")
        return "Данные от первой задачи"

    @task
    def task_two(input_from_task_one: str):
        """Вторая простая задача, получающая данные от первой."""
        print(f"Вторая задача получила: '{input_from_task_one}'")

    # Установка зависимостей
    result = task_one()
    task_two(result)

# Инстанцирование DAG
daily_cron_dag()

