"""DAG: случайно выбирает источник фактов (dogs/cats) и через BranchOperator пишет в разные таблицы Postgres.

Логика:
1) choose_endpoint() — рандомно выбирает один endpoint и забирает данные
2) branch_by_source — выбирает ветку: load_dog_fact или load_cat_fact
3) load_* — создаёт таблицу (если нет) и вставляет данные

Требования:
- Connection в Airflow берётся из Variable POSTGRES_CONN_ID (по умолчанию postgres_data)
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from typing import Any, Dict, Optional

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG(
    dag_id="5_2_pet_facts_branch",
    default_args=default_args,
    description="Random cats/dogs facts -> branching -> Postgres",
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=["api", "postgres", "branch", "basic_pipeline"],
)


def _safe_get_first_cat_fact(payload: Any) -> Optional[Dict[str, Any]]:
    """cat-fact.herokuapp.com/facts может вернуть список или объект; пытаемся взять один факт."""
    if isinstance(payload, list):
        if not payload:
            return None
        item = payload[0]
        return item if isinstance(item, dict) else None

    if isinstance(payload, dict):
        # Иногда ответ может быть вида {"all": [...]} или {"facts": [...]} — подстрахуемся
        for key in ("all", "facts", "data"):
            if key in payload and isinstance(payload[key], list) and payload[key]:
                first = payload[key][0]
                return first if isinstance(first, dict) else None
        return payload

    return None


@task(task_id="choose_endpoint", dag=dag)
def choose_endpoint() -> Dict[str, Any]:
    """Случайно выбирает cats или dogs endpoint, получает данные и нормализует результат.

    Примечание: cats endpoint cat-fact.herokuapp.com иногда отдаёт 503, поэтому есть fallback на catfact.ninja.
    """

    source = random.choice(["dogs", "cats"])

    if source == "dogs":
        url = "https://api.sykes.pet/fact"
        last_err: Optional[Exception] = None
        for _ in range(3):
            try:
                r = requests.get(url, timeout=20)
                r.raise_for_status()
                data = r.json()

                fact = None
                if isinstance(data, dict):
                    fact = data.get("fact") or data.get("text") or data.get("message")

                if not fact:
                    raise ValueError(f"Unexpected dogs payload: {data}")

                return {
                    "source": "dogs",
                    "fact": str(fact),
                    "raw": data,
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                }
            except Exception as e:
                last_err = e

        raise RuntimeError(f"Dogs endpoint failed after retries: {url}") from last_err

    # cats
    cats_urls = [
        "https://cat-fact.herokuapp.com/facts",  # целевой из задания
        "https://catfact.ninja/fact",  # fallback (более стабильный)
    ]

    last_err = None
    for url in cats_urls:
        for _ in range(3):
            try:
                r = requests.get(url, timeout=20)
                r.raise_for_status()
                data = r.json()

                # fallback endpoint catfact.ninja
                if url.endswith("/fact") and isinstance(data, dict) and "fact" in data:
                    fact = data.get("fact")
                    length = data.get("length")
                    return {
                        "source": "cats",
                        "fact": str(fact),
                        "length": int(length) if isinstance(length, int) else (len(str(fact)) if fact else None),
                        "raw": data,
                        "fetched_at": datetime.now(timezone.utc).isoformat(),
                    }

                # основной herokuapp
                item = _safe_get_first_cat_fact(data)
                if not item:
                    raise ValueError(f"Unexpected cats payload: {data}")

                fact = item.get("text") or item.get("fact")
                if not fact:
                    raise ValueError(f"Unexpected cats fact item: {item}")

                length = item.get("length")
                if length is None:
                    length = len(str(fact))

                return {
                    "source": "cats",
                    "fact": str(fact),
                    "length": int(length) if str(length).isdigit() else None,
                    "raw": item,
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                }
            except Exception as e:
                last_err = e

    raise RuntimeError("Cats endpoints failed after retries") from last_err


def _branch_by_source(ti, **_kwargs):
    payload = ti.xcom_pull(task_ids="choose_endpoint")
    source = (payload or {}).get("source")

    if source == "dogs":
        return "load_dog_fact"
    if source == "cats":
        return "load_cat_fact"

    raise ValueError(f"Unknown source in XCom: {payload}")


branch_by_source = BranchPythonOperator(
    task_id="branch_by_source",
    python_callable=_branch_by_source,
    dag=dag,
)


POSTGRES_CONN_ID = Variable.get("POSTGRES_CONN_ID", default_var="postgres_data")


@task(task_id="load_dog_fact", dag=dag)
def load_dog_fact(payload: Dict[str, Any]) -> int:
    """Создаёт таблицу dog_facts и пишет факт."""
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dog_facts (
            id SERIAL PRIMARY KEY,
            fact TEXT NOT NULL,
            fetched_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    conn.commit()

    cur.execute(
        """
        INSERT INTO dog_facts (fact, fetched_at)
        VALUES (%s, %s)
        RETURNING id;
        """,
        (payload["fact"], payload["fetched_at"]),
    )
    inserted_id = cur.fetchone()[0]
    conn.commit()

    cur.close()
    conn.close()
    return int(inserted_id)


@task(task_id="load_cat_fact", dag=dag)
def load_cat_fact(payload: Dict[str, Any]) -> int:
    """Создаёт таблицу cat_facts_alt и пишет факт."""
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS cat_facts_alt (
            id SERIAL PRIMARY KEY,
            fact TEXT NOT NULL,
            length INTEGER,
            fetched_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    conn.commit()

    cur.execute(
        """
        INSERT INTO cat_facts_alt (fact, length, fetched_at)
        VALUES (%s, %s, %s)
        RETURNING id;
        """,
        (payload["fact"], payload.get("length"), payload["fetched_at"]),
    )
    inserted_id = cur.fetchone()[0]
    conn.commit()

    cur.close()
    conn.close()
    return int(inserted_id)


payload_arg = choose_endpoint()

load_dogs = load_dog_fact(payload_arg)
load_cats = load_cat_fact(payload_arg)

branch_by_source.set_upstream(payload_arg)
load_dogs.set_upstream(branch_by_source)
load_cats.set_upstream(branch_by_source)

