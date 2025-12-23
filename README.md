# Учебный проект Airflow

Этот репозиторий содержит набор учебных DAG'ов, демонстрирующих различные концепции и подходы к разработке в Apache Airflow.

## Быстрый старт

Для запуска проекта и развёртывания всех сервисов (Airflow, Postgres, MinIO) выполните следующие шаги.

### 1. Запуск Docker Compose

Перейдите в корневую директорию проекта и выполните команду:

```bash
# Убедитесь, что вы находитесь в папке /home/envy/Lessons/airflow
docker compose up -d
```

Эта команда запустит все необходимые сервисы в фоновом режиме.

### 2. Настройка окружения Airflow

После запуска контейнеров необходимо настроить переменные (Variables) и подключения (Connections), которые используются в DAG'ах. Для этого существует специальный скрипт.

Выполните команду:

```bash
# Этот скрипт добавит все необходимые conn/var в Airflow
./scripts/bootstrap_airflow.sh
```

После выполнения скрипта ваше окружение будет полностью готово к работе.

### Доступ к сервисам

- **Airflow UI**: [http://localhost:8080](http://localhost:8080) (логин/пароль: `airflow`/`airflow`)
- **MinIO UI**: [http://localhost:9001](http://localhost:9001) (логин/пароль: `minioadmin`/`minioadmin`)

---

## Полезные ссылки из документации Airflow

### Основы и UI
- [Обзор UI](https://airflow.apache.org/docs/apache-airflow/2.8.1/ui.html) — Знакомство с интерфейсом Airflow.
- [Кастомизация UI](https://airflow.apache.org/docs/apache-airflow/2.8.1/howto/customize-ui.html) — Как настроить внешний вид UI.
- [Конфигурация Flask](https://airflow.apache.org/docs/apache-airflow/2.8.1/howto/set-config.html#configuring-flask-application-for-airflow-webserver) — Настройка веб-сервера Airflow.
- [Основы (Tutorial)](https://airflow.apache.org/docs/apache-airflow/2.8.1/tutorial/fundamentals.html) — Фундаментальный туториал по Airflow.
- [Лучшие практики](https://airflow.apache.org/docs/apache-airflow/2.8.1/best-practices.html) — Рекомендации по написанию DAG'ов.

### Ключевые концепции
- [Задачи (Tasks)](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html) — Всё о задачах.
- [Операторы (Operators)](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/operators.html) — Основные строительные блоки DAG'ов.
- [Сенсоры (Sensors)](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html) — Операторы, которые ожидают наступления события.
- [DAG Run](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dag-run.html#passing-parameters-when-triggering-dags) — Всё о запусках DAG'а и передаче параметров.
- [Правила запуска (Trigger Rules)](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#concepts-trigger-rules) — Как управлять логикой выполнения задач.
- [Backfill и Catchup](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dag-run.html#backfill) — Как выполнять пропущенные запуски DAG'ов.

### Написание DAG'ов и планирование
- [Написание и планирование (общий раздел)](https://airflow.apache.org/docs/apache-airflow/2.8.1/authoring-and-scheduling/index.html) — Главный раздел по созданию DAG'ов.
- [Часовые пояса (Timezone)](https://airflow.apache.org/docs/apache-airflow/2.8.1/authoring-and-scheduling/timezone.html) — Как правильно работать с часовыми поясами.
- [Датасеты (Datasets)](https://airflow.apache.org/docs/apache-airflow/2.8.1/authoring-and-scheduling/datasets.html) — Планирование на основе данных.
- [Расписания (Timetables)](https://airflow.apache.org/docs/apache-airflow/2.8.1/authoring-and-scheduling/timetable.html) — Создание кастомных расписаний.

### Администрирование и производительность
- [Планировщик (Scheduler)](https://airflow.apache.org/docs/apache-airflow/2.8.1/administration-and-deployment/scheduler.html) — Как работает планировщик Airflow.
- [Обработка DAG-файлов](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dagfile-processing.html#fine-tuning-your-dag-processor-performance) — Оптимизация производительности обработки DAG'ов.

### Продвинутые темы
- [Task SDK](https://airflow.apache.org/docs/task-sdk/stable/index.html) — SDK для взаимодействия с задачами.
- [Backfill (детально)](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/backfill.html) — Детальное описание команды `backfill`.

