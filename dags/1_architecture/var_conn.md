# Переменные и подключения (Variables & Connections)

В Apache Airflow для передачи конфигурации и параметров во время выполнения используются два основных механизма:

* **Variables** — произвольные runtime-параметры (ключ/значение)
* **Connections** — параметры подключения к внешним системам (БД, API, очереди и т.д.)

---

# Variables

**Variables** — это механизм конфигурации Airflow во время выполнения (*runtime configuration*).
Они представляют собой **глобальное хранилище пар ключ/значение**, доступное из любых DAG’ов и задач.

### Основные свойства Variables:

* Глобальные для всего Airflow
* Доступны из Python-кода DAG’ов и операторов
* Настраиваются через UI или JSON-файл
* Подходят для:

  * флагов
  * параметров запуска
  * пороговых значений
  * небольших конфигураций

---

## Использование Variables в коде

```python
from airflow.models import Variable

# Обычное получение значения
foo = Variable.get("foo")

# Автоматическая десериализация JSON-значения
bar = Variable.get("bar", deserialize_json=True)

# Возвращает default_var (None), если переменная не задана
baz = Variable.get("baz", default_var=None)
```

---

## Важные замечания по Variables

* Переменные **хранятся в базе Airflow в открытом виде**
* **Не рекомендуется** хранить:

  * пароли
  * токены
  * секреты
* Частые обращения к `Variable.get()` **во время парсинга DAG’ов** могут замедлять Scheduler

---

# Connections

**Connections** — это механизм Airflow для хранения параметров подключения к внешним системам.

Обычно используются для:

* баз данных (PostgreSQL, MySQL, Oracle)
* очередей (Kafka, RabbitMQ)
* облачных сервисов (AWS, GCP, Azure)
* REST API
* файловых хранилищ (S3, MinIO, HDFS)

---

## Что хранит Connection

Connection — это структурированный объект, включающий:

* `conn_id` — уникальный идентификатор подключения
* `conn_type` — тип подключения (postgres, mysql, http, aws и т.д.)
* `host`
* `schema`
* `login`
* `password`
* `port`
* `extra` — JSON-поле с дополнительными параметрами

---

## Использование Connections в коде

### Получение подключения вручную

```python
from airflow.hooks.base import BaseHook

conn = BaseHook.get_connection("my_postgres_conn")

host = conn.host
login = conn.login
password = conn.password
schema = conn.schema
port = conn.port
extra = conn.extra_dejson
```

---

### Использование через Operators и Hooks

На практике Connections чаще используются **неявно** — через Operators и Hooks:

```python
PostgresOperator(
    task_id="run_query",
    postgres_conn_id="my_postgres_conn",
    sql="SELECT 1;"
)
```

В этом случае Airflow сам:

* извлекает Connection
* передаёт параметры в Hook
* устанавливает соединение

---

## Где настраиваются Connections

Connections можно задать:

* через **Web UI**
  *Admin → Connections*
* через **Environment Variables**
* через **Secrets Backend** (Vault, AWS Secrets Manager и т.д.)
* через CLI:

```bash
airflow connections add
```

---

## Variables vs Connections — сравнение

| Критерий          | Variables          | Connections                       |
| ----------------- | ------------------ | --------------------------------- |
| Назначение        | Runtime-параметры  | Подключения к системам            |
| Структура         | Ключ / значение    | Структурированный объект          |
| Хранение секретов | ❌ Не рекомендуется | ✅ Предназначено                   |
| Использование     | `Variable.get()`   | Hooks / Operators                 |
| Безопасность      | Низкая             | Выше (особенно с Secrets Backend) |

---

## Best Practices

### Используйте Variables для:

* feature flags
* порогов и коэффициентов
* логических параметров
* небольших JSON-конфигураций

### Используйте Connections для:

* паролей
* API-токенов
* строк подключения
* доступа к внешним системам

---

## Важно для продакшена

* Не храните секреты в Variables
* Используйте **Secrets Backend** для Connections
* Минимизируйте вызовы `Variable.get()` при парсинге DAG’ов
* Всегда задавайте `default_var`, если переменная может отсутствовать

