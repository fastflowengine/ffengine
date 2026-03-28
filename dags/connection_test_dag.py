"""
3 test DB bağlantısını doğrulayan örnek DAG.
Her task ilgili DB'den ff_test_data tablosundaki satır sayısını okur.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.sdk import DAG

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # pragma: no cover
    from airflow.operators.python import PythonOperator

from ffengine.airflow.operator import resolve_dialect
from ffengine.db.airflow_adapter import AirflowConnectionAdapter
from ffengine.db.session import DBSession


POSTGRES_CONN_ID = os.getenv("FFENGINE_TEST_POSTGRES_CONN_ID", "test_postgres")
MSSQL_CONN_ID = os.getenv("FFENGINE_TEST_MSSQL_CONN_ID", "test_mssql")
ORACLE_CONN_ID = os.getenv("FFENGINE_TEST_ORACLE_CONN_ID", "test_oracle")


def _count_rows(*, conn_id: str, label: str) -> dict[str, int | str]:
    params = AirflowConnectionAdapter.get_connection_params(conn_id)
    dialect = resolve_dialect(params["conn_type"])
    q_table = dialect.quote_identifier("ff_test_data")

    with DBSession(params, dialect) as session:
        cursor = session.cursor()
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {q_table}")
            row = cursor.fetchone()
        finally:
            cursor.close()

    rows = int(row[0]) if row else 0
    return {"database": label, "conn_id": conn_id, "rows": rows}


with DAG(
    dag_id="ffengine_connection_test",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ffengine", "test"],
    doc_md=(
        "3 test DB bağlantısını doğrular: PostgreSQL, MSSQL, Oracle. "
        "Conn ID değerleri ortam değişkenleri ile override edilebilir: "
        "`FFENGINE_TEST_POSTGRES_CONN_ID`, `FFENGINE_TEST_MSSQL_CONN_ID`, "
        "`FFENGINE_TEST_ORACLE_CONN_ID`."
    ),
) as dag:
    test_postgres = PythonOperator(
        task_id="test_postgres",
        python_callable=_count_rows,
        op_kwargs={"conn_id": POSTGRES_CONN_ID, "label": "postgres"},
    )

    test_mssql = PythonOperator(
        task_id="test_mssql",
        python_callable=_count_rows,
        op_kwargs={"conn_id": MSSQL_CONN_ID, "label": "mssql"},
    )

    test_oracle = PythonOperator(
        task_id="test_oracle",
        python_callable=_count_rows,
        op_kwargs={"conn_id": ORACLE_CONN_ID, "label": "oracle"},
    )

    [test_postgres, test_mssql, test_oracle]
