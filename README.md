# Учебный проект Airflow

Этот репозиторий содержит набор учебных DAG'ов, демонстрирующих различные концепции и подходы к разработке в Apache Airflow.

## Технологический стек

- **Airflow**: 2.10.4 (standalone mode с LocalExecutor)
- **Python**: 3.12
- **Postgres**: 17 для Airflow, 15 для учебных данных (две инстанции)
- **MinIO**: latest (S3-совместимое хранилище)

## Быстрый старт

Для запуска проекта и развёртывания всех сервисов выполните следующие шаги.

### 1. Первый запуск (инициализация)

При первом запуске необходимо собрать образ Docker и инициализировать базу данных Airflow:

```bash
# Убедитесь, что вы находитесь в папке /home/envy/Lessons/airflow

# 1. Соберите Docker образ (займёт 2-3 минуты)
docker compose build

# 2. Запустите все сервисы
docker compose up -d

# 3. Проверьте статус (Airflow standalone может запускаться до 1 минуты)
docker compose ps

# 4. Проверьте логи Airflow (опционально)
docker compose logs -f airflow-standalone
```

**Важно:** При первом запуске Airflow автоматически:
- Инициализирует базу данных
- Создаст пользователя `airflow` с паролем `airflow`
- Запустит webserver, scheduler и triggerer в одном контейнере

### 2. Последующие запуски

Для последующих запусков достаточно одной команды:

```bash
docker compose up -d
```

Эта команда запустит все необходимые сервисы в фоновом режиме.

### 3. Настройка окружения Airflow

После запуска контейнеров и успешной инициализации Airflow (проверьте что UI доступен на http://localhost:8080) необходимо настроить переменные (Variables) и подключения (Connections), которые используются в DAG'ах.

Выполните команду:

```bash
# Этот скрипт добавит все необходимые conn/var в Airflow
./scripts/bootstrap_airflow.sh
```

**Примечание:** Скрипт должен выполниться без ошибок. Если вы видите `service "airflow-scheduler" is not running`, значит Airflow ещё не запустился полностью. Подождите 30-60 секунд и повторите команду.

После выполнения скрипта ваше окружение будет полностью готово к работе.

---

## Устранение проблем

### ⚠️ Важно: Конфликт версий провайдеров

**Проблема:** Если при запуске `airflow-init` вы видите ошибку `airflow: command not found`, это означает конфликт версий.

**Причина:** В `requirements.txt` указаны провайдеры с версиями, несовместимыми с Airflow 2.10.4. Например, `apache-airflow-providers-amazon>=8.0.0` может установить Airflow 3.x поверх 2.10.4, что сломает установку.

**Решение:** В requirements.txt используйте **конкретные версии** провайдеров, совместимые с Airflow 2.10.4:
```
apache-airflow-providers-amazon==8.25.0
apache-airflow-providers-postgres==5.11.2
apache-airflow-providers-http==4.10.1
```

После исправления пересоберите образ:
```bash
docker compose down
docker compose build --no-cache
docker compose up -d
```

### Airflow UI недоступен на localhost:8080

1. Проверьте статус контейнера:
```bash
docker ps | grep airflow-standalone
```

2. Проверьте логи:
```bash
docker logs airflow-airflow-standalone-1 --tail 50
```

3. Проверьте что airflow-init завершился успешно:
```bash
docker logs airflow-airflow-init-1 | grep -E "(Admin user|Airflow version)"
```

Должны увидеть:
```
Admin user airflow created
Airflow version
2.10.4
```

### bootstrap_airflow.sh выдаёт ошибку

Убедитесь что:
1. Airflow полностью запустился (это может занять 1-2 минуты после `docker compose up -d`)
2. Проверьте доступность: `curl http://localhost:8080/health`
3. Контейнер `airflow-standalone` в статусе `healthy`: `docker ps | grep healthy`

### Как проверить что всё работает

```bash
# 1. Все контейнеры запущены
docker compose ps

# 2. Airflow отвечает
curl http://localhost:8080/health

# 3. Можно войти в UI
# Откройте http://localhost:8080 в браузере
# Логин: airflow, Пароль: airflow
```

### Доступ к сервисам

- **Airflow UI**: [http://localhost:8080](http://localhost:8080) (логин/пароль: `airflow`/`airflow`)
- **MinIO UI**: [http://localhost:9001](http://localhost:9001) (логин/пароль: `minioadmin`/`minioadmin`)
- **Postgres (Airflow)**: `localhost:5432` (user: `airflow`, password: `airflow`, db: `airflow`)
- **Postgres (Data)**: `localhost:5433` (user: `postgres`, password: `postgres`, db: `postgres`)

### Управление сервисами

```bash
# Остановить все сервисы
docker compose down

# Остановить с удалением volumes (⚠️ удалит все данные!)
docker compose down -v

# Посмотреть логи
docker compose logs -f airflow-standalone

# Перезапустить Airflow без пересоздания контейнера
docker compose restart airflow-standalone

# Пересоздать контейнер (например, после изменения Dockerfile)
docker compose up -d --build
```

## Особенности standalone режима

В этом проекте используется **Airflow Standalone** — упрощённый режим развёртывания, который:

- Запускает все компоненты Airflow (webserver, scheduler, triggerer) в одном контейнере.
- Использует **LocalExecutor** вместо Celery — задачи выполняются локально, без очереди и воркеров.
- Идеален для разработки, обучения и небольших проектов.
- Не требует Redis или отдельных контейнеров для scheduler/worker.

**Преимущества:**
- Простота настройки и быстрый старт.
- Меньше потребление ресурсов (один контейнер вместо 4-5).
- Легче дебажить и следить за логами.

**Ограничения:**
- Не подходит для production с высокой нагрузкой.
- Все задачи выполняются последовательно в рамках LocalExecutor.

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

