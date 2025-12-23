"""
Пример простого DAG для демонстрации работы `catchup=True`.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from airflow.decorators import dag, task

@dag(
    dag_id="3_1_catchup_example",
    description="Простой DAG для демонстрации catchup=True",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 12, 10),
    catchup=True,
    tags=["tutorial", "catchup"],
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
)
def catchup_demonstration_dag():
    @task
    def print_execution_date_task_1(**kwargs):
        """
        Эта задача выводит в лог свою 'data_interval_start',
        чтобы показать, за какой период времени она запущена.
        """
        execution_date = kwargs['data_interval_start']
        print(f"Task 1: Запуск за дату (data_interval_start): {execution_date}")
        return str(execution_date)

    @task
    def print_execution_date_task_2(date_from_task_1: str, **kwargs):
        """
        Эта задача делает то же самое и подтверждает получение данных от первой.
        """
        execution_date = kwargs['data_interval_start']
        print(f"Task 2: Запуск за дату (data_interval_start): {execution_date}")
        print(f"Task 2: Получено от первой задачи: {date_from_task_1}")

    # Создаем зависимость между задачами
    date_value = print_execution_date_task_1()
    print_execution_date_task_2(date_value)

# Инстанцируем DAG
catchup_demonstration_dag()
