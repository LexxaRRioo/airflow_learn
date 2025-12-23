"""
DAG с инициализацией через TaskFlow API
Логика: 2 таски отправляют данные в XCom, 3-я таска их читает.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from airflow.decorators import dag, task

@dag(
    dag_id="2_3_xcom_taskflow_init",
    description="Пример DAG с 3 тасками и XCom (TaskFlow API)",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["tutorial", "xcom", "taskflow"],
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
)
def xcom_taskflow_dag():
    @task
    def push_task_1():
        """Возвращает простое строковое значение, которое попадёт в XCom."""
        return 'Hello from Task 1'

    @task
    def push_task_2():
        """Возвращает словарь, который попадёт в XCom."""
        return {'user': 'airflow', 'id': 123}

    @task
    def pull_task(value1: str, value2: dict):
        """Принимает результаты предыдущих тасок через аргументы."""
        print(f"Получено из task_1: {value1}")
        print(f"Получено из task_2: {value2}")

        if not value1 or not value2:
            raise ValueError("Не удалось получить данные из XCom")

    # Вызов функций создаёт задачи и неявно связывает их
    value_from_task_1 = push_task_1()
    value_from_task_2 = push_task_2()
    pull_task(value_from_task_1, value_from_task_2)

# Инстанцирование DAG
xcom_taskflow_dag()

