import os

import pytest

from ffengine.db.session import DBSession
from ffengine.dialects import MSSQLDialect, OracleDialect
from ffengine.mapping.resolver import MappingResolver
from ffengine.pipeline.target_writer import TargetWriter


_ENABLED = os.getenv("FFENGINE_ENABLE_CROSS_DB_TESTS", "0").strip() == "1"
pytestmark = [pytest.mark.integration]
if not _ENABLED:
    pytestmark.append(
        pytest.mark.skip(reason="FFENGINE_ENABLE_CROSS_DB_TESTS=1 olmadigi icin skip.")
    )


def _mssql_params() -> dict:
    return {
        "host": os.getenv("MSSQL_TEST_HOST", "localhost"),
        "port": int(os.getenv("MSSQL_TEST_PORT", "1433")),
        "user": os.getenv("MSSQL_TEST_USER", "sa"),
        "password": os.getenv(
            "MSSQL_TEST_PASS", os.getenv("MSSQL_SA_PASS", "Mssql_password123!")
        ),
        "database": os.getenv("MSSQL_TEST_DB", "ffengine_test"),
        "driver": os.getenv("MSSQL_TEST_DRIVER", "{ODBC Driver 17 for SQL Server}"),
        "extra": {
            "Encrypt": os.getenv("MSSQL_TEST_ENCRYPT", "no"),
            "TrustServerCertificate": os.getenv("MSSQL_TEST_TRUST_SERVER_CERT", "yes"),
        },
    }


def _oracle_params() -> dict:
    return {
        "host": os.getenv("ORACLE_TEST_HOST", "localhost"),
        "port": int(os.getenv("ORACLE_TEST_PORT", "1521")),
        "user": os.getenv("ORACLE_TEST_USER", "ffengine"),
        "password": os.getenv(
            "ORACLE_TEST_PASS", os.getenv("ORACLE_PASS", "Oracle_password123!")
        ),
        "database": os.getenv("ORACLE_TEST_SERVICE", "FREEPDB1"),
    }


def _drop_mssql_table(session: DBSession, schema: str, table: str) -> None:
    cursor = session.cursor(server_side=False)
    try:
        cursor.execute(f"DROP TABLE IF EXISTS [{schema}].[{table}]")
        session.conn.commit()
    except Exception:
        session.conn.rollback()
    finally:
        cursor.close()


def _drop_oracle_table(session: DBSession, schema: str, table: str) -> None:
    cursor = session.cursor(server_side=False)
    try:
        cursor.execute(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE "
            + f'"{schema}"."{table}"'
            + "'; EXCEPTION WHEN OTHERS THEN NULL; END;"
        )
        session.conn.commit()
    except Exception:
        session.conn.rollback()
    finally:
        cursor.close()


def test_mssql_to_oracle_auto_ddl_preserves_lengths_and_bounded_max_policy():
    src_schema = "ffengine"
    src_table = "ff_auto_ddl_type_src"
    tgt_schema = "FFENGINE"
    tgt_table = "FF_AUTO_DDL_TYPE_TGT"

    src_dialect = MSSQLDialect()
    tgt_dialect = OracleDialect()

    with DBSession(_mssql_params(), src_dialect) as src_session:
        with DBSession(_oracle_params(), tgt_dialect) as tgt_session:
            try:
                _drop_mssql_table(src_session, src_schema, src_table)
                _drop_oracle_table(tgt_session, tgt_schema, tgt_table)

                cur_src = src_session.cursor(server_side=False)
                try:
                    cur_src.execute(
                        f"""
                        CREATE TABLE [{src_schema}].[{src_table}] (
                            id INT NOT NULL,
                            city NVARCHAR(120) NOT NULL,
                            iata CHAR(3) NOT NULL,
                            notes NVARCHAR(MAX) NULL,
                            amount DECIMAL(12,2) NOT NULL
                        )
                        """
                    )
                    src_session.conn.commit()
                finally:
                    cur_src.close()

                task_config = {
                    "source_schema": src_schema,
                    "source_table": src_table,
                    "target_schema": tgt_schema,
                    "target_table": tgt_table,
                    "column_mapping_mode": "source",
                    "passthrough_full": True,
                    "load_method": "create_if_not_exists_or_truncate",
                }

                mapping = MappingResolver().resolve(
                    task_config,
                    src_session.conn,
                    src_dialect,
                    tgt_dialect,
                )
                task_config["source_columns"] = mapping.source_columns
                task_config["target_columns"] = mapping.target_columns
                task_config["target_columns_meta"] = mapping.target_columns_meta

                TargetWriter(tgt_session, tgt_dialect).prepare(task_config)

                cur_tgt = tgt_session.cursor(server_side=False)
                try:
                    cur_tgt.execute(
                        """
                        SELECT COLUMN_NAME,
                               DATA_TYPE,
                               CHAR_COL_DECL_LENGTH,
                               DATA_PRECISION,
                               DATA_SCALE
                        FROM   ALL_TAB_COLUMNS
                        WHERE  OWNER = :1
                          AND  TABLE_NAME = :2
                        ORDER  BY COLUMN_ID
                        """,
                        (tgt_schema, tgt_table),
                    )
                    rows = cur_tgt.fetchall()
                finally:
                    cur_tgt.close()

                by_col = {str(r[0]).upper(): r for r in rows}

                city = by_col["CITY"]
                assert str(city[1]).upper() == "VARCHAR2"
                assert int(city[2]) == 120

                iata = by_col["IATA"]
                assert str(iata[1]).upper() == "CHAR"
                assert int(iata[2]) == 3

                notes = by_col["NOTES"]
                assert str(notes[1]).upper() == "VARCHAR2"
                assert int(notes[2]) == 4000

                amount = by_col["AMOUNT"]
                assert str(amount[1]).upper() == "NUMBER"
                assert int(amount[3]) == 12
                assert int(amount[4]) == 2
            finally:
                _drop_mssql_table(src_session, src_schema, src_table)
                _drop_oracle_table(tgt_session, tgt_schema, tgt_table)
