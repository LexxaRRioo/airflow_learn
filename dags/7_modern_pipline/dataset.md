## Быстрый старт

Помимо планирования DAG на основе времени, их можно планировать на основе обновления задачи какого-либо **dataset**.

```python
from airflow.datasets import Dataset

with DAG(...):
    MyOperator(
        # эта задача обновляет example.csv
        outlets=[Dataset("s3://dataset-bucket/example.csv")],
        ...,
    )

with DAG(
    # этот DAG должен запускаться, когда example.csv обновляется (dag1)
    schedule=[Dataset("s3://dataset-bucket/example.csv")],
    ...,
):
    ...
```

![](../_images/dataset-scheduled-dags.png)

---

## Что такое “dataset”?

**Dataset** в Airflow — это логическая группа данных. Dataset может обновляться "производящей" задачей (**producer task**), а обновление dataset служит сигналом для запуска "потребляющих" DAG (**consumer DAG**).

**Dataset** определяется через URI:

```python
from airflow.datasets import Dataset

example_dataset = Dataset("s3://dataset-bucket/example.csv")
```

Airflow не делает предположений о содержимом или местоположении данных, представленных URI. Он рассматривается как строка, поэтому попытка использовать регулярные выражения (`input_\d+.csv`) или шаблоны (`input_2022*.csv`) для создания нескольких dataset из одной декларации не сработает.

---

## Валидные URI

Dataset должен иметь **валидный URI**. Airflow core и провайдеры определяют разные схемы URI, например:

* `file` (core)
* `postgres` (Postgres provider)
* `s3` (Amazon provider)

Третьи провайдеры и плагины могут предоставлять свои схемы. Предопределённые схемы имеют собственную семантику, которую нужно соблюдать.

URI должен соответствовать RFC 3986:
ASCII-символы, `%`, `-`, `_`, `.`, `~`.
URI **чувствителен к регистру**, включая часть host.

Примеры **невалидных dataset**:

```python
reserved = Dataset("airflow://example_dataset")
not_ascii = Dataset("èxample_datašet")
```

Примеры **валидных dataset**:

```python
my_ds = Dataset("x-my-thing://foobarbaz")
schemeless = Dataset("//example/dataset")
csv_file = Dataset("example_dataset")
```

---

## Дополнительная информация

Можно добавить словарь `extra` в Dataset:

```python
example_dataset = Dataset(
    "s3://dataset/example.csv",
    extra={"team": "trainees"},
)
```

`extra` не влияет на идентичность dataset — DAG будет триггериться по URI, даже если `extra` отличается.

---

## Использование dataset в DAG

Пример:

```python
example_dataset = Dataset("s3://dataset/example.csv")

with DAG(dag_id="producer", ...):
    BashOperator(task_id="producer", outlets=[example_dataset], ...)

with DAG(dag_id="consumer", schedule=[example_dataset], ...):
    ...
```

* DAG **consumer** запустится только после успешного завершения задачи **producer**.
* Если задача не выполнена или пропущена, dataset не обновляется, DAG не запускается.

---

## Несколько dataset

Параметр `schedule` — это список, поэтому DAG может требовать несколько dataset. DAG будет запущен только когда **все dataset**, которые он потребляет, обновились хотя бы один раз с последнего запуска:

```python
with DAG(
    dag_id="multiple_datasets_example",
    schedule=[
        example_dataset_1,
        example_dataset_2,
        example_dataset_3,
    ],
    ...,
):
    ...
```

---

## Получение информации из Dataset, вызвавшего DAG

DAG может использовать шаблон `triggering_dataset_events` для получения информации о dataset, который его вызвал.

```python
example_snowflake_dataset = Dataset("snowflake://my_db.my_schema.my_table")

with DAG(dag_id="load_snowflake_data", schedule="@hourly", ...):
    SQLExecuteQueryOperator(
        task_id="load",
        conn_id="snowflake_default",
        outlets=[example_snowflake_dataset],
        ...
    )

with DAG(dag_id="query_snowflake_data", schedule=[example_snowflake_dataset], ...):
    SQLExecuteQueryOperator(
        task_id="query",
        conn_id="snowflake_default",
        sql="""
          SELECT *
          FROM my_db.my_schema.my_table
          WHERE "updated_at" >= '{{ (triggering_dataset_events.values() | first | first).source_dag_run.data_interval_start }}'
          AND "updated_at" < '{{ (triggering_dataset_events.values() | first | first).source_dag_run.data_interval_end }}';
        """,
    )

    @task
    def print_triggering_dataset_events(triggering_dataset_events=None):
        for dataset, dataset_list in triggering_dataset_events.items():
            print(dataset, dataset_list)
            print(dataset_list[0].source_dag_run.dag_id)

    print_triggering_dataset_events()
```

---

## Продвинутое планирование с условными выражениями

Airflow поддерживает **логические операторы для dataset**:

* **AND (`&`)** — DAG запускается только когда все указанные dataset обновлены.
* **OR (`|`)** — DAG запускается если хотя бы один dataset обновлен.

### Примеры

**Запуск при обновлении всех dataset:**

```python
dag1_dataset = Dataset("s3://dag1/output_1.txt")
dag2_dataset = Dataset("s3://dag2/output_1.txt")

with DAG(
    schedule=(dag1_dataset & dag2_dataset),
    ...,
):
    ...
```

**Запуск при обновлении любого dataset:**

```python
with DAG(
    schedule=(dag1_dataset | dag2_dataset),
    ...,
):
    ...
```

**Сложная логика:**

```python
dag3_dataset = Dataset("s3://dag3/output_3.txt")

with DAG(
    schedule=(dag1_dataset | (dag2_dataset & dag3_dataset)),
    ...,
):
    ...
```

---

## Комбинирование с тайм-планом

С помощью **DatasetTimetable** можно комбинировать **планирование на основе данных** и **по времени**, чтобы DAG запускался как по событиям dataset, так и по фиксированному расписанию.

