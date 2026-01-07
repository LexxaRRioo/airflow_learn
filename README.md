# Airflow Project

Этот проект содержит настроенную среду Apache Airflow для запуска ETL-пайплайнов.

## Как учиться по этому репозиторию

### Порядок изучения материалов

Начните с просмотра видео: [https://youtu.be/Dx7WLsInkbE?si=t2we6gX2einMrh8Z](https://youtu.be/Dx7WLsInkbE?si=t2we6gX2einMrh8Z)

Читайте markdown файлы в следующем порядке, параллельно изучая соответствующие Python файлы:

**1. Архитектура Airflow** (`dags/1_architecture/`)
- [scheduler.md](dags/1_architecture/scheduler.md) - Как работает планировщик
- [ui.md](dags/1_architecture/ui.md) - Интерфейс пользователя
- [var_conn.md](dags/1_architecture/var_conn.md) - Variables и Connections
- [xcom.md](dags/1_architecture/xcom.md) - Обмен данными между задачами
- **Executors** (`dags/1_architecture/executors/`)
  - [0_architecture.md](dags/1_architecture/executors/0_architecture.md) - Общая архитектура
  - [1_executors.md](dags/1_architecture/executors/1_executors.md) - Типы исполнителей
  - [2_CeleryExecutor.md](dags/1_architecture/executors/2_CeleryExecutor.md) - Distributed executor
  - [3_LocalExecutor.md](dags/1_architecture/executors/3_LocalExecutor.md) - Локальный executor
  - [4_another_executor.md](dags/1_architecture/executors/4_another_executor.md) - Другие executors

**2. Основы создания DAG** (`dags/2_basic_init_dag/`)
- [0_1_init_dag.md](dags/2_basic_init_dag/0_1_init_dag.md) - Инициализация DAG
- [0_2_task.md](dags/2_basic_init_dag/0_2_task.md) - Создание задач

**3. Расписание и catchup** (`dags/3_catchup_start_date/`)
- [0_1_dug_run.md](dags/3_catchup_start_date/0_1_dug_run.md) - DAG Runs
- [0_2_cron_preset.md](dags/3_catchup_start_date/0_2_cron_preset.md) - Cron и preset расписания

**4. Управление потоком выполнения** (`dags/4_trigger_rules/`)
- [trigger_rules.md](dags/4_trigger_rules/trigger_rules.md) - Правила запуска задач

**5. Базовые пайплайны** (`dags/5_basic_pipeline/`)
- [sensors.md](dags/5_basic_pipeline/sensors.md) - Сенсоры для ожидания событий

**6. Legacy подход к зависимостям** (`dags/6_legacy_pipline/`)
- [dag_to_dag_dependency.md](dags/6_legacy_pipline/dag_to_dag_dependency.md) - Старый способ связи DAG-ов

**7. Современный подход** (`dags/7_modern_pipline/`)
- [README.md](dags/7_modern_pipline/README.md) - Обзор современного подхода
- [dataset.md](dags/7_modern_pipline/dataset.md) - Datasets (рекомендуемый способ)
- [asset.md](dags/7_modern_pipline/asset.md) - Assets

### Как просматривать Markdown в VS Code

**Способ 1: Горячие клавиши (рекомендуется)**
1. Откройте любой `.md` файл
2. Нажмите (на английской раскладке):
   - **Mac:** `⌘ + K`, отпустите, затем нажмите `V`
   - **Windows:** `Ctrl + K`, отпустите, затем нажмите `V`
3. Файл откроется в split-view: слева код, справа preview

```
┌─────────────────────────────────────────────┐
│  scheduler.md (code)  │  Preview (rendered) │
│  # Scheduler          │  Scheduler          │
│  Планировщик...       │  Планировщик...     │
│                       │  [link] ← clickable │
└─────────────────────────────────────────────┘
```

**Способ 2: Command Palette**
1. `⌘ + Shift + P` (Mac) / `Ctrl + Shift + P` (Windows)
2. Введите: `Markdown: Open Preview to the Side`
3. Enter

**Способ 3: Иконка в углу**
- В правом верхнем углу редактора найдите иконку книги (Open Preview to the Side)

**Навигация по ссылкам:**
- **В preview:** просто кликните на ссылку
- **В коде:** `⌘ + Click` (Mac) / `Ctrl + Click` (Windows)

### Workflow обучения

```
┌─────────────────────────────────────────────────────────────┐
│ 1. docker compose up -d --wait                              │
├─────────────────────────────────────────────────────────────┤
│ 2. VS Code: Откройте папку проекта                          │
├─────────────────────────────────────────────────────────────┤
│ 3. Параллельная работа с материалами:                       │
│    ┌──────────────────────┬──────────────────────┐          │
│    │ scheduler.md (⌘+K V) │ 1_scheduler_dag.py   │          │
│    │ (preview mode)       │ (код примера)        │          │
│    └──────────────────────┴──────────────────────┘          │
├─────────────────────────────────────────────────────────────┤
│ 4. Браузер: localhost:8080 (airflow/airflow)                │
│    Смотрите как работают DAG-и в реальном времени           │
├─────────────────────────────────────────────────────────────┤
│ 5. Экспериментируйте: меняйте код → сохраняйте → смотрите   │
│    как изменился DAG в UI (обновляется автоматически)       │
└─────────────────────────────────────────────────────────────┘

Порядок изучения: 1_architecture → 2_basic_init_dag → ... → 7_modern_pipline
```

## Запуск проекта

0. **Скачайте и установите Docker desktop**

    [https://docs.docker.com/desktop/](https://docs.docker.com/desktop/)

    Возможные проблемы:
    - [не включена виртуализация по умолчанию (можешь следовать этому гайду сверху-вниз до "Первый запуск Docker Desktop" включительно)](https://www.securitylab.ru/blog/personal/Neurosinaps/355103.php)
    - [команды docker не работают в терминале](https://lib.osipenkov.ru/reshenie-problemy-s-ustanovkoj-docker-desktop-na-windows/) + не забудьте перезапустить терминал (закрыть-открыть)

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

