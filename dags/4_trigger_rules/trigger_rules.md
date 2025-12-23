# Trigger Rules в Airflow

Trigger Rule определяет, **при каком состоянии upstream-задач текущая задача будет запущена**.

| Trigger Rule                  | Описание                                                                                     |
| ----------------------------- | -------------------------------------------------------------------------------------------- |
| `all_success` (по умолчанию)  | Все upstream-задачи должны завершиться успешно                                               |
| `all_failed`                  | Все upstream-задачи должны быть в состоянии `failed` или `upstream_failed`                   |
| `all_done`                    | Все upstream-задачи должны завершиться (любым образом: `success`, `failed`, `skipped`)       |
| `all_skipped`                 | Все upstream-задачи должны быть пропущены (`skipped`)                                        |
| `one_failed`                  | Хотя бы одна upstream-задача завершилась с ошибкой (не ждёт остальных)                       |
| `one_success`                 | Хотя бы одна upstream-задача завершилась успешно (не ждёт остальных)                         |
| `one_done`                    | Хотя бы одна upstream-задача завершилась (`success` или `failed`)                            |
| `none_failed`                 | Ни одна upstream-задача не завершилась с ошибкой (все либо `success`, либо `skipped`)        |
| `none_failed_min_one_success` | Нет ошибок, и хотя бы одна upstream-задача успешна                                           |
| `none_skipped`                | Ни одна upstream-задача не была пропущена (`success`, `failed`, `upstream_failed` допустимы) |
| `always`                      | Без зависимостей — задача запускается всегда                                                 |


---

# Примеры

Для примеров возьмём `TriggerRule.ONE_FAILED`
(остальные настраиваются аналогично).

---

## 1️⃣ Operator style (Object style)

Классический способ через оператор.

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

def task_func():
    print("Task executed")

with DAG(
    dag_id="operator_style_trigger_rule",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="run_on_one_failed",
        python_callable=task_func,
        trigger_rule=TriggerRule.ONE_FAILED,
    )
```

📌 **Используется чаще всего в legacy-DAG’ах**

---

## 2️⃣ TaskFlow style (TaskFlow API)

Современный и рекомендуемый стиль.

```python
from airflow import DAG
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

with DAG(
    dag_id="taskflow_trigger_rule",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
):

    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def run_on_one_failed():
        print("Task executed")

    run_on_one_failed()
```

📌 **Самый чистый и читаемый вариант**

---

## 3️⃣ Context style (через context / kwargs)

Пример доступа к контексту выполнения.

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

def task_with_context(**context):
    ti = context["ti"]
    dag_run = context["dag_run"]
    print(f"Triggered by DAG run: {dag_run.run_id}")

with DAG(
    dag_id="context_style_trigger_rule",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="run_on_one_failed",
        python_callable=task_with_context,
        trigger_rule=TriggerRule.ONE_FAILED,
        provide_context=True,
    )
```

📌 Используется, когда:

* нужен `task_instance`
* нужен `dag_run`
* нужен доступ к XCom / execution context

---

# Важно помнить (часто спрашивают на собеседовании)

* `trigger_rule` **применяется только к upstream-задачам**
* `one_failed` и `one_success` **не ждут завершения всех upstream**
* `all_done` — самый безопасный вариант для cleanup-задач
* `always` игнорирует зависимости полностью

---