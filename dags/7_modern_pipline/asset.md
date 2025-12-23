# Планирование на основе активов (Asset-Aware Scheduling)

**Добавлено в версии 2.4**

---

## Быстрый старт

Помимо планирования DAG на основе времени, вы можете планировать DAG так, чтобы они запускались при обновлении определённого **актива (asset)**.

```python
from airflow.sdk import DAG, Asset

with DAG(...):
    MyOperator(
        # эта задача обновляет example.csv
        outlets=[Asset("s3://asset-bucket/example.csv")],
        ...,
    )

with DAG(
    # этот DAG должен запускаться, когда example.csv обновляется (dag1)
    schedule=[Asset("s3://asset-bucket/example.csv")],
    ...,
):
    ...
```

![](../_images/asset_scheduled_dags.png)

Подробнее: см. **Asset Definitions** — как объявлять активы.

---

## Планирование DAG с активами

Активы используются для задания зависимости DAG от данных. Пример:

```python
example_asset = Asset("s3://asset/example.csv")

with DAG(dag_id="producer", ...):
    BashOperator(task_id="producer", outlets=[example_asset], ...)

with DAG(dag_id="consumer", schedule=[example_asset], ...):
    ...
```

* DAG **consumer** запускается только после успешного выполнения задачи **producer**.
* Актив помечается как обновлённый только если задача завершилась успешно.
* Если задача пропущена или завершилась с ошибкой, обновления не происходит, DAG не запускается.

Список связей между активами и DAG можно посмотреть в **Asset Views**.

---

## Несколько активов

Параметр `schedule` — это список, поэтому DAG может требовать несколько активов. DAG запускается после того, как **все активы, которые он потребляет, были обновлены хотя бы один раз с последнего запуска**:

```python
with DAG(
    dag_id="multiple_assets_example",
    schedule=[
        example_asset_1,
        example_asset_2,
        example_asset_3,
    ],
    ...,
):
    ...
```

Если один актив обновляется несколько раз до обновления всех потребляемых активов, downstream DAG всё равно запускается только один раз.

---

## Получение информации о событии, вызвавшем DAG

DAG может получать информацию об активе, который его вызвал, через `triggering_asset_events` (шаблон или параметр).

Пример структуры `triggering_asset_events`:

```python
{
    Asset("s3://asset-bucket/example.csv"): [
        AssetEvent(uri="s3://asset-bucket/example.csv", source_dag_run=DagRun(...), ...),
        ...
    ],
    Asset("s3://another-bucket/another.csv"): [
        AssetEvent(uri="s3://another-bucket/another.csv", source_dag_run=DagRun(...), ...),
        ...
    ],
}
```

Эти данные можно использовать через **Jinja-шаблоны** или напрямую в Python.

---

### Доступ к событиям через Jinja

**Пример для одного актива:**

```python
example_snowflake_asset = Asset("snowflake://my_db/my_schema/my_table")

with DAG(dag_id="query_snowflake_data", schedule=[example_snowflake_asset], ...):
    SQLExecuteQueryOperator(
        task_id="query",
        conn_id="snowflake_default",
        sql="""
          SELECT *
          FROM my_db.my_schema.my_table
          WHERE "updated_at" >= '{{ (triggering_asset_events.values() | first | first).source_dag_run.data_interval_start }}'
          AND "updated_at" < '{{ (triggering_asset_events.values() | first | first).source_dag_run.data_interval_end }}';
        """,
    )
```

**Пример для нескольких активов:**

```python
with DAG(dag_id="process_assets", schedule=[asset1, asset2], ...):
    BashOperator(
        task_id="process",
        bash_command="""
        {% for asset_uri, events in triggering_asset_events.items() %}
          echo "Processing asset: {{ asset_uri }}"
          {% for event in events %}
            echo "  Triggered by DAG: {{ event.source_dag_run.dag_id }}"
            echo "  Data interval start: {{ event.source_dag_run.data_interval_start }}"
            echo "  Data interval end: {{ event.source_dag_run.data_interval_end }}"
          {% endfor %}
        {% endfor %}
        """,
    )
```

---

### Доступ к событиям в Python

```python
@task
def print_triggering_asset_events(triggering_asset_events=None):
    if triggering_asset_events:
        for asset, asset_events in triggering_asset_events.items():
            print(f"Asset: {asset.uri}")
            for event in asset_events:
                print(f"  - Triggered by DAG run: {event.source_dag_run.dag_id}")
                print(f"    Data interval: {event.source_dag_run.data_interval_start} to {event.source_dag_run.data_interval_end}")
                print(f"    Run ID: {event.source_dag_run.run_id}")
                print(f"    Timestamp: {event.timestamp}")

print_triggering_asset_events()
```

> Примечание: при нескольких событиях для одного актива логика обработки может быть сложной. Автор DAG решает, как их обрабатывать.

---

## Event-driven планирование

Asset-based планирование DAG, триггерящееся другими DAG (internal event-driven), не покрывает все сценарии. Иногда нужно запускать DAG от **внешних событий**: сигналов системы, сообщений, изменений данных в реальном времени.

Airflow поддерживает два подхода:

### 1. Push-based (REST API)

Добавлено в версии 2.9.
Внешние системы могут отправлять события активов в Airflow через REST API. События помещаются в очередь (`queued asset events`), и DAG запускается, когда все требуемые активы обновлены.

Примеры API эндпоинтов:

* Получить queued asset event для DAG: `/assets/queuedEvent/{uri}`
* Получить queued asset events для DAG: `/dags/{dag_id}/assets/queuedEvent`
* Удалить queued asset event для DAG: `/assets/queuedEvent/{uri}`
* Удалить queued asset events для DAG: `/dags/{dag_id}/assets/queuedEvent`
* Получить queued asset events для актива: `/dags/{dag_id}/assets/queuedEvent/{uri}`
* Удалить queued asset events для актива: `DELETE /dags/{dag_id}/assets/queuedEvent/{uri}`

---

### 2. Pull-based (Asset Watchers)

Airflow может сам получать события из внешних источников через **AssetWatcher**.

* AssetWatcher следит за внешним источником (очередь, хранилище).
* При событии актив обновляется и запускается DAG.
* Совместимо только с триггерами, наследующими `BaseEventTrigger`, чтобы избежать бесконечного реседулинга.

---

## Продвинутое планирование с условными выражениями

Airflow поддерживает **логические операторы для активов**:

* **AND (`&`)** — DAG запускается только после обновления всех указанных активов.
* **OR (`|`)** — DAG запускается после обновления хотя бы одного актива.

### Примеры

**Запуск при обновлении всех активов:**

```python
dag1_asset = Asset("s3://dag1/output_1.txt")
dag2_asset = Asset("s3://dag2/output_1.txt")

with DAG(schedule=(dag1_asset & dag2_asset), ...):
    ...
```

**Запуск при обновлении любого актива:**

```python
with DAG(schedule=(dag1_asset | dag2_asset), ...):
    ...
```

**Сложная логика (один актив или оба других):**

```python
dag3_asset = Asset("s3://dag3/output_3.txt")

with DAG(schedule=(dag1_asset | (dag2_asset & dag3_asset)), ...):
    ...
```

---

## Планирование на основе алиасов активов

* События активов, добавленных к алиасу, обрабатываются как обычные активы.
* DAG downstream может зависеть от алиаса, при этом события активов учитываются для планирования.
* DAG триггерится, если хотя бы один актив связан с алиасом в runtime.

Пример:

```python
with DAG(dag_id="asset-producer"):
    @task(outlets=[Asset("example-alias")])
    def produce_asset_events():
        pass

with DAG(dag_id="asset-alias-producer"):
    @task(outlets=[AssetAlias("example-alias")])
    def produce_asset_events(*, outlet_events):
        outlet_events[AssetAlias("example-alias")].add(Asset("s3://bucket/my-task"))

with DAG(dag_id="asset-consumer", schedule=Asset("s3://bucket/my-task")):
    ...

with DAG(dag_id="asset-alias-consumer", schedule=AssetAlias("example-alias")):
    ...
```

> После выполнения `asset-alias-producer` алиас `AssetAlias("example-alias")` разрешается в `Asset("s3://bucket/my-task")`. DAG `asset-alias-consumer` обновит расписание при следующем разборе DAG.

---

## Комбинирование активов и тайм-планов

С помощью **AssetOrTimeSchedule** можно комбинировать **события активов** и **периодическое расписание**. Это позволяет DAG запускаться и по обновлениям данных, и по фиксированному расписанию.
