"""DAG B (Dataset): cats_processor_dataset_dag

Consumer на Airflow Datasets.

Запускается при появлении события Dataset (из producer DAG).

Простой подход: Dataset — это просто сигнал "данные готовы".
Consumer знает соглашение (bucket/prefix) и сам находит последний файл.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


MINIO_CONN_ID = Variable.get("MINIO_CONN_ID", default_var="minio")
POSTGRES_CONN_ID = Variable.get("POSTGRES_CONN_ID", default_var="postgres_data")
RAW_BUCKET = Variable.get("MINIO_BUCKET_CATS", default_var="cats")
RAW_PREFIX = Variable.get("MINIO_RAW_PREFIX", default_var="raw")

CATS_RAW_DATASET = Dataset(f"minio://{RAW_BUCKET}/{RAW_PREFIX}")
TARGET_TABLE = "cat_images"


@dag(
    dag_id="7_2_cats_processor_dataset_dag",
    description="Consumer (Dataset): берёт последний файл из MinIO -> transform -> Postgres",
    schedule=[CATS_RAW_DATASET],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["cats", "minio", "postgres", "dataset", "consumer", "modern_pipeline","basic_pipeline"],
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
)
def cats_processor_dataset_dag():
    @task
    def read_latest_from_minio() -> Dict[str, Any]:
        """Находит последний файл в MinIO (по timestamp в имени) и читает его.

        Dataset событие — это просто сигнал "иди проверь".
        Мы знаем соглашение: bucket=cats, prefix=raw/, файлы с timestamp.
        """
        s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)

        # Получаем список всех ключей в prefix
        keys = s3.list_keys(bucket_name=RAW_BUCKET, prefix=RAW_PREFIX) or []

        if not keys:
            raise FileNotFoundError(
                f"Нет файлов в s3://{RAW_BUCKET}/{RAW_PREFIX}. "
                "Producer DAG должен сначала создать данные."
            )

        # Берём последний (timestamp в имени файла обеспечивает правильную сортировку)
        latest_key = sorted(keys)[-1]

        print(f"📂 Найдено файлов: {len(keys)}")
        print(f"✅ Выбран последний: s3://{RAW_BUCKET}/{latest_key}")

        # Читаем содержимое
        body = s3.read_key(key=latest_key, bucket_name=RAW_BUCKET)
        if body is None:
            raise FileNotFoundError(f"Не удалось прочитать s3://{RAW_BUCKET}/{latest_key}")

        raw = json.loads(body)
        print(f"📊 Прочитано записей: {len(raw.get('items', []))}")

        return raw

    @task
    def transform(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Трансформирует raw данные в формат для Postgres."""
        items = raw.get("items")
        if not isinstance(items, list):
            raise ValueError(f"Ожидался список items, получено: {type(items)}")

        load_dt = datetime.now(timezone.utc)

        result = []
        for item in items:
            if not isinstance(item, dict):
                continue

            cat_id = item.get("id")
            url = item.get("url")

            if not cat_id or not url:
                continue

            result.append({
                "id": cat_id,
                "url": url,
                "width": item.get("width"),
                "height": item.get("height"),
                "load_dt": load_dt,
            })

        print(f"✅ Трансформировано записей: {len(result)}")
        return result

    @task
    def load_to_postgres(rows: List[Dict[str, Any]]) -> int:
        """Создаёт таблицу (если нет) и загружает данные в Postgres."""
        if not rows:
            print("⚠️ Нет данных для загрузки")
            return 0

        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg.get_conn()
        cur = conn.cursor()

        # Создаём таблицу
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                id TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                width INTEGER,
                height INTEGER,
                load_dt TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()

        # Upsert данных
        cur.executemany(f"""
            INSERT INTO {TARGET_TABLE} (id, url, width, height, load_dt)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                url = EXCLUDED.url,
                width = EXCLUDED.width,
                height = EXCLUDED.height,
                load_dt = EXCLUDED.load_dt;
        """, [
            (r["id"], r["url"], r["width"], r["height"], r["load_dt"])
            for r in rows
        ])

        conn.commit()
        inserted = len(rows)

        cur.close()
        conn.close()

        print(f"✅ Загружено/обновлено записей: {inserted}")
        return inserted

    # Определяем pipeline
    raw_data = read_latest_from_minio()
    transformed = transform(raw_data)
    load_to_postgres(transformed)


# Инстанцируем DAG
cats_processor_dataset_dag()




