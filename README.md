# Airflow Project

Этот проект содержит настроенную среду Apache Airflow для запуска ETL-пайплайнов.

## Запуск проекта

1.  **Соберите и запустите Docker контейнеры:**

    Выполните следующую команду в корневой директории проекта, чтобы запустить (поднять) все сервисы в фоновом режиме. Запустятся Airflow, Postgres, MinIO.

    ```bash
    docker compose up --build -d --wait
    ```

    Если пишет, что какой-то порт уже занят, например `Bind for 0.0.0.0:8080 failed: port is already allocated`,
    перейди в папку с другим проектом и выполни 

    ```bash
    docker compose down
    ```

    Проверь через команду, что ничего лишнего не запущено, и пробуй "поднять контейнеры" ещё раз первой командой с `up`.

    ```bash
    docker ps
    ```

2.  **Доступ к веб-интерфейсу Airflow:**

    Откройте браузер и перейдите по адресу [http://localhost:8080](http://localhost:8080).

    *   **Логин:** `airflow`
    *   **Пароль:** `airflow`

3. ** Доступ к другим сервисам (опционально)
    * MinIO Console:  http://localhost:9001 (minioadmin/minioadmin)"
    * MinIO API:      http://localhost:9000"
    * PostgreSQL:     localhost:5432 (airflow/airflow)"

## Полезные ссылки на документацию

### Основные концепции
*   [UI Overview](https://airflow.apache.org/docs/apache-airflow/2.8.1/ui.html) - Обзор пользовательского интерфейса Airflow.
*   [Fundamentals Tutorial](https://airflow.apache.org/docs/apache-airflow/2.8.1/tutorial/fundamentals.html) - Основы работы с Airflow.
*   [DAGs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#concepts-trigger-rules) - Описание DAG'ов и правил их запуска (Trigger Rules).
*   [Tasks](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html) - Задачи в Airflow.
*   [Operators](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/operators.html) - Операторы.
*   [Sensors](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html) - Сенсоры.
*   [DAG Runs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dag-run.html#passing-parameters-when-triggering-dags) - Запуски DAG'ов и передача параметров.
*   [Backfill](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/backfill.html) - Процесс выполнения прошедших запусков DAG.

### Разработка и планирование
*   [Authoring and Scheduling](https://airflow.apache.org/docs/apache-airflow/2.8.1/authoring-and-scheduling/index.html) - Руководство по созданию и планированию DAG'ов.
*   [Best Practices](https://airflow.apache.org/docs/apache-airflow/2.8.1/best-practices.html) - Лучшие практики при работе с Airflow.
*   [Timezone](https://airflow.apache.org/docs/apache-airflow/2.8.1/authoring-and-scheduling/timezone.html) - Работа с часовыми поясами.
*   [Datasets](https://airflow.apache.org/docs/apache-airflow/2.8.1/authoring-and-scheduling/datasets.html) - Использование датасетов для запуска DAG'ов.
*   [Timetables](https://airflow.apache.org/docs/apache-airflow/2.8.1/authoring-and-scheduling/timetable.html) - Пользовательские расписания.
*   [Task SDK](https://airflow.apache.org/docs/task-sdk/stable/index.html) - SDK для задач.

### Администрирование и развертывание
*   [Scheduler](https://airflow.apache.org/docs/apache-airflow/2.8.1/administration-and-deployment/scheduler.html) - Как работает планировщик.
*   [DAG File Processing](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dagfile-processing.html#fine-tuning-your-dag-processor-performance) - Оптимизация производительности обработки DAG-файлов.
*   [Customize UI](https://airflow.apache.org/docs/apache-airflow/2.8.1/howto/customize-ui.html) - Кастомизация веб-интерфейса.
*   [Set Config](https://airflow.apache.org/docs/apache-airflow/2.8.1/howto/set-config.html#configuring-flask-application-for-airflow-webserver) - Настройка конфигурации веб-сервера.

