"""
F3.3 integration: aktarım muhasebesi (K1) gerçek PostgreSQL üzerinde.

Kritik satır T-F3.3-2: writer kasıtlı satır düşürdüğünde partition
fail-loud kapanır (`ReconciliationError`) ve — dosya hedeflerindeki
promotion emsalinde olduğu gibi — akış tamamlanmış sayılmaz. Ayrıca mutlu
yolda sayaçların gerçek veriyle tuttuğu ve muhasebenin kaynağa **ek sorgu
atmadığı** doğrulanır (T-F3.3-1 / T-F3.3-4).

Run: FFENGINE_ENABLE_PG_TESTS=1 pytest tests/integration/test_pg_reconciliation.py -v
"""

from __future__ import annotations

import os

import pytest

from ffengine.errors.exceptions import ReconciliationError


def _should_skip():
    if os.getenv("FFENGINE_ENABLE_PG_TESTS", "0").strip() != "1":
        return True, "FFENGINE_ENABLE_PG_TESTS=1 olmadığı için skip."
    return False, ""


_SKIP, _SKIP_REASON = _should_skip()
pytestmark = [pytest.mark.integration]
if _SKIP:
    pytestmark.append(pytest.mark.skip(reason=_SKIP_REASON))


@pytest.fixture(scope="module")
def pg_conn_params():
    return {
        "host": os.getenv("PG_TEST_HOST", "localhost"),
        "port": int(os.getenv("PG_TEST_PORT", "5435")),
        "user": os.getenv("POSTGRES_TEST_USER", "ffengine_test"),
        "password": os.getenv("POSTGRES_TEST_PASS", "ffengine_pg_pass"),
        "database": os.getenv("POSTGRES_TEST_DB", "ffengine_test_db"),
    }


@pytest.fixture(scope="module")
def pg_dialect():
    from ffengine.dialects import PostgresDialect

    return PostgresDialect()


@pytest.fixture(scope="module")
def sessions(pg_conn_params, pg_dialect):
    from ffengine.db.session import DBSession

    with DBSession(pg_conn_params, pg_dialect) as src, \
            DBSession(pg_conn_params, pg_dialect) as tgt:
        yield src, tgt


_SRC = "ff_rec_src"
_TGT = "ff_rec_tgt"


def _drop(session, names):
    try:
        session.conn.rollback()
    except Exception:
        pass
    cur = session.cursor()
    try:
        for n in names:
            cur.execute(f"DROP TABLE IF EXISTS public.{n}")
        session.conn.commit()
    except Exception:
        session.conn.rollback()
    finally:
        cur.close()


def _exec(session, *stmts):
    cur = session.cursor()
    try:
        for s in stmts:
            cur.execute(s)
        session.conn.commit()
    finally:
        cur.close()


def _scalar(session, sql):
    cur = session.cursor()
    try:
        cur.execute(sql)
        return cur.fetchone()[0]
    finally:
        cur.close()


@pytest.fixture(autouse=True)
def seed(sessions):
    src, tgt = sessions
    _drop(src, [_SRC])
    _drop(tgt, [_TGT])
    _exec(
        src,
        f"CREATE TABLE public.{_SRC} (id int primary key, name varchar(50))",
        f"INSERT INTO public.{_SRC} (id, name) SELECT g, 'row-' || g "
        f"FROM generate_series(1, 25) g",
    )
    _exec(
        tgt,
        f"CREATE TABLE public.{_TGT} (id int primary key, name varchar(50))",
    )
    yield
    _drop(src, [_SRC])
    _drop(tgt, [_TGT])


def _task_config():
    return {
        "task_group_id": "recon_task",
        "load_method": "append",
        "source_schema": "public",
        "source_table": _SRC,
        "source_columns": ["id", "name"],
        "target_schema": "public",
        "target_table": _TGT,
        "target_columns": ["id", "name"],
        "target_columns_meta": [],
        # Birden fazla chunk üret: muhasebe chunk'lar boyunca toplanmalı.
        "batch_size": 10,
    }


def test_reconciliation_passes_on_real_transfer(sessions, pg_dialect):
    """T-F3.3-1/T-F3.3-4 canlı: sayaçlar gerçek veriyle tutar ve muhasebe
    kaynağa ek sorgu atmaz (hedefteki satır sayısı sayaçla birebir)."""
    from ffengine.core.flow_manager import FlowManager

    src, tgt = sessions
    result = FlowManager().run_flow_task(
        src_session=src,
        tgt_session=tgt,
        src_dialect=pg_dialect,
        tgt_dialect=pg_dialect,
        task_config=_task_config(),
    )

    assert result.rows_read == 25
    assert result.rows_written == 25
    assert result.rows_rejected == 0
    assert result.reconciliation_status == "passed"
    assert _scalar(tgt, f"SELECT count(*) FROM public.{_TGT}") == 25


def test_deliberate_writer_row_loss_fails_partition(
    sessions, pg_dialect, monkeypatch
):
    """T-F3.3-2 (INV-1) canlı: writer her chunk'ta bir satırı sessizce
    düşürürse partition ReconciliationError ile fail-loud kapanır."""
    from ffengine.core.flow_manager import FlowManager
    from ffengine.pipeline.target_writer import TargetWriter

    original = TargetWriter.write_batch

    def lossy_write_batch(self, rows, task_config):
        # Gerçek yazma yapılır ama SON SATIR atlanır: hem hedefte gerçek
        # kayıp oluşur hem de writer bunu dürüstçe raporlar.
        kept = list(rows)[:-1]
        if not kept:
            return 0
        return original(self, kept, task_config)

    monkeypatch.setattr(TargetWriter, "write_batch", lossy_write_batch)

    src, tgt = sessions
    with pytest.raises(ReconciliationError) as exc:
        FlowManager().run_flow_task(
            src_session=src,
            tgt_session=tgt,
            src_dialect=pg_dialect,
            tgt_dialect=pg_dialect,
            task_config=_task_config(),
        )

    details = exc.value.details
    assert details["rows_read"] == 25
    assert details["rows_written"] < 25
    assert details["delta"] == details["rows_read"] - details["rows_written"]
    # Actionable + secret-free: sayaç/kimlik var, veri satırı ve SQL yok.
    message = str(exc.value)
    assert "okunan" in message and "yazilan" in message
    assert "row-" not in message and "INSERT" not in message.upper()

    # Hedefteki fiili satır sayısı da kayıplı — muhasebe gerçeği yakaladı.
    landed = _scalar(tgt, f"SELECT count(*) FROM public.{_TGT}")
    assert landed == details["rows_written"] < 25
