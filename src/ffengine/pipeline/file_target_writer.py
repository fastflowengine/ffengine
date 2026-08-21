"""
F1.5 — File target writer (DB/file rows → delimited file on local/SFTP).

Implements the same duck-typed writer contract the DB ``TargetWriter`` exposes
(``prepare`` / ``write_batch`` / ``rollback_batch``) plus a ``finalize`` hook the
``Streamer`` calls after the last batch. Writing streams a temp file and, only on
success, promotes it atomically (temp→rename) so an interrupted upload never
leaves a partial final file (INV-1). M=1: a single writer instance per transfer
(file targets are forced to one partition; a second ``prepare`` fails loud).
"""

from __future__ import annotations

import csv
import datetime as _dt
import decimal
import io
import json
import os
import re

from ffengine.errors.exceptions import FileTargetError
from ffengine.pipeline.file_transport import open_write

# Hedef dosya formatlari. `csv` varsayilandir (geriye uyum: `target_file_format`
# tasimayan mevcut configler aynen CSV yazar).
FORMAT_CSV = "csv"
FORMAT_JSON = "json"
VALID_TARGET_FILE_FORMATS = frozenset({FORMAT_CSV, FORMAT_JSON})


def _json_default(value):
    """JSON'a serilestirilemeyen tipler icin donusum (fail-loud).

    CSV yolu her degeri str()'e cevirir; json.dumps ise Decimal/date/datetime/
    bytes icin TypeError atar -- DB kaynakli satirlarda Decimal neredeyse her
    numeric kolonda vardir.

    `Decimal` -> str: float'a cevirmek sessiz precision kaybidir (INV-1).
    Bilinmeyen tip -> fail-loud; sessiz str() ile veriyi bozmayiz.
    """
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, (_dt.datetime, _dt.date, _dt.time)):
        return value.isoformat()
    raise FileTargetError(
        f"JSON hedefine yazilamayan deger tipi: {type(value).__name__}. "
        "Desteklenenler: metin, sayi, bool, null, Decimal, date/datetime. "
        "Binary kolonlar icin CSV formatini kullanin."
    )


class FileTargetWriter:
    def __init__(self, file_ctx):
        self.ctx = file_ctx
        self.options = dict(getattr(file_ctx, "options", {}) or {})
        self._handle = None
        self._columns: list[str] = []
        self._encoding = "utf-8"
        self._delimiter = ","
        self._format = FORMAT_CSV
        self._rows_written = 0

    # ------------------------------------------------------------------
    # Writer contract
    # ------------------------------------------------------------------

    def prepare(self, task_config: dict) -> None:
        if self._handle is not None:
            raise FileTargetError(
                "Dosya hedefi tek yazicidir (M=1); prepare tekrar cagrilamaz."
            )
        self._columns = list(task_config.get("target_columns") or [])
        if not self._columns:
            raise FileTargetError(
                "target_columns bos: dosya hedefi en az bir kolon gerektirir."
            )
        self._encoding = str(self.options.get("encoding") or "utf-8")
        self._delimiter = str(self.options.get("delimiter") or ",")
        self._format = _resolve_format(self.options.get("format"))
        self._handle = open_write(
            self.ctx.conn_id,
            self.ctx.conn_type,
            self.ctx.file_path,
            tmp_suffix=_tmp_suffix(task_config),
        )
        # Header yalniz CSV'de anlamlidir: JSON (JSONL) her satirda kolon
        # adlarini anahtar olarak tasir, ayri bir baslik satiri yoktur.
        if self._format == FORMAT_CSV and self.options.get("header", True):
            self._handle.stream.write(self._encode_rows([self._columns]))

    def write_batch(self, rows: list[tuple], task_config: dict) -> int:
        if not rows:
            return 0
        if self._handle is None:
            raise FileTargetError("write_batch prepare() oncesi cagrildi.")
        self._handle.stream.write(self._encode_rows(rows))
        self._rows_written += len(rows)
        return len(rows)

    def finalize(self) -> None:
        """Promote the temp file onto the final path (atomic)."""
        if self._handle is None:
            return
        handle, self._handle = self._handle, None
        handle.promote()

    def rollback_batch(self) -> None:
        self.abort()

    def abort(self) -> None:
        """Drop the temp file; the final path is never touched."""
        if self._handle is None:
            return
        handle, self._handle = self._handle, None
        handle.abort()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _encode_rows(self, rows) -> bytes:
        if self._format == FORMAT_JSON:
            return self._encode_json(rows)
        return self._encode_csv(rows)

    def _encode_csv(self, rows) -> bytes:
        sio = io.StringIO()
        writer = csv.writer(sio, delimiter=self._delimiter, lineterminator="\n")
        writer.writerows(rows)
        return sio.getvalue().encode(self._encoding)

    def _encode_json(self, rows) -> bytes:
        """JSONL: satir basina tek JSON obje.

        Tek buyuk JSON array DEGIL. Uc gerekce: (1) kaynak okuyucu
        (`file_source_reader._read_json_flat`) JSONL bekler -- kendi ciktimizi
        geri okuyabilmeliyiz; (2) writer batch batch akitir, array acilis/kapanis
        parantezi finalize'a icerik yazma sorumlulugu bindirir ve abort'ta yarim
        dosya riski dogurur; (3) sabit-RAM sozlesmesi korunur.

        `ensure_ascii=False`: aksi halde `target_encoding: utf-8` anlamsizlasir
        ve Turkce karakterler `\\u00e7` olarak kacar. Anahtar sirasi
        `target_columns` sirasidir (dict ekleme sirasini korur; sort_keys YOK).
        """
        sio = io.StringIO()
        for row in rows:
            obj = dict(zip(self._columns, row))
            sio.write(json.dumps(obj, ensure_ascii=False, default=_json_default))
            sio.write("\n")
        return sio.getvalue().encode(self._encoding)


def _resolve_format(value) -> str:
    """Hedef dosya formatini coz (verilmezse CSV -- geriye uyum)."""
    name = str(value or "").strip().lower() or FORMAT_CSV
    if name not in VALID_TARGET_FILE_FORMATS:
        raise FileTargetError(
            f"Gecersiz target_file_format: '{name}'. "
            f"Gecerli degerler: {sorted(VALID_TARGET_FILE_FORMATS)}."
        )
    return name


def _tmp_suffix(task_config: dict) -> str:
    run_id = str(
        task_config.get("run_id") or task_config.get("_run_id") or ""
    ).strip()
    token = run_id or f"pid{os.getpid()}"
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", token)
    return f".fftmp-{safe}"
