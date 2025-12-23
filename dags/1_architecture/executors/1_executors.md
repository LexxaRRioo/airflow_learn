# Типы Executor’ов (Executor Types)

В Airflow существует **два типа executor’ов**:

1. Те, которые выполняют задачи **локально** (внутри процесса scheduler’а)
2. Те, которые выполняют задачи **удалённо** (обычно через пул worker’ов)

По умолчанию Airflow настроен с **SequentialExecutor** — это локальный executor и самый простой вариант выполнения.
Однако **SequentialExecutor не подходит для продакшена**, так как:

* он не поддерживает параллельное выполнение задач;
* из-за этого некоторые возможности Airflow (например, сенсоры) работают некорректно.

Вместо него рекомендуется:

* использовать **LocalExecutor** для небольших продакшн-установок на одной машине;
* использовать **удалённые executor’ы** для многоузловых или облачных инсталляций.

---

## Локальные Executor’ы (Local Executors)

* Debug Executor *(устарел)*
* Local Executor
* Sequential Executor

---

## Удалённые Executor’ы (Remote Executors)

* Celery Executor
* CeleryKubernetes Executor
* Dask Executor
* Kubernetes Executor
* LocalKubernetes Executor

---



