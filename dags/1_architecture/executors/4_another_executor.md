
# AWS Batch Executor

Executor на базе **Amazon Batch**, где каждая задача запускается в отдельном контейнере.

### Преимущества:

* Масштабируемость и снижение стоимости
* Очереди и приоритеты
* Поддержка:

  * Fargate (ECS)
  * EC2
  * EKS
* Быстрый старт задач без холодного старта

---

# Edge Executor

**EdgeExecutor** используется для распределения задач между worker’ами, находящимися в разных локациях.

* Может использоваться параллельно с другими executor’ами
* Требует отдельной настройки Edge Worker’ов
* Параметры — в Edge provider Configuration Reference

---

## Очереди — EdgeExecutor

* `queue` задаётся на уровне task или DAG
* Очередь по умолчанию:

```ini
[operators]
default_queue = default
```

### Запуск worker’а:

```bash
airflow edge worker -q remote,wisconsin_site
```

* Если `queue` не указан — worker слушает все очереди
* При использовании нескольких executor’ов:
* EdgeExecutor нужно явно указывать на уровне DAG или task
