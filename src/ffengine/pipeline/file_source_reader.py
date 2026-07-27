"""
F1.4 — File source reader (WHAT the bytes mean).

Reads ``csv`` and ``json`` (flat/JSONL) files as constant-RAM chunks of
``list[tuple]`` — the exact batch contract the DB ``SourceReader`` produces, so
``Streamer``/``TargetWriter`` stay untouched. The byte source is provided by the
F1.5 transport (local ``fs`` / ``sftp``); this module only parses. Rows are
emitted in ``source_columns`` order (explicit mapping is mandatory). A corrupt
row / nested-in-flat value fails loud with the file + line number (INV-1).
"""

from __future__ import annotations

import csv
import json
from typing import Generator

from ffengine.errors.exceptions import FileSourceError
from ffengine.pipeline.file_transport import open_read, resolve_read_paths


class FileSourceReader:
    def __init__(self, file_ctx, config: dict):
        self.ctx = file_ctx
        self.config = config
        self.batch_size = int(config.get("batch_size", 10_000) or 10_000)
        self.source_columns = list(config.get("source_columns") or [])
        self.options = dict(getattr(file_ctx, "options", {}) or {})

    # ------------------------------------------------------------------
    # Public API — mirrors SourceReader.read()
    # ------------------------------------------------------------------

    def read(self) -> Generator[list[tuple], None, None]:
        if not self.source_columns:
            raise FileSourceError(
                "Dosya kaynagi icin source_columns bos olamaz "
                "(explicit mapping gerekli)."
            )
        paths = resolve_read_paths(
            self.ctx.conn_id, self.ctx.conn_type, self.ctx.file_path
        )
        buffer: list[tuple] = []
        for path in paths:
            for row in self._read_one(path):
                buffer.append(row)
                if len(buffer) >= self.batch_size:
                    yield buffer
                    buffer = []
        if buffer:
            yield buffer

    # ------------------------------------------------------------------
    # Per-file parsing
    # ------------------------------------------------------------------

    def _read_one(self, path: str) -> Generator[tuple, None, None]:
        handle = open_read(self.ctx.conn_id, self.ctx.conn_type, path)
        encoding = str(self.options.get("encoding") or "utf-8")
        lines = _decode_lines(handle.stream, encoding)
        try:
            if self.ctx.source_type == "csv":
                yield from self._read_csv(lines, path)
            else:
                yield from self._read_json_flat(lines, path)
        finally:
            handle.close()

    def _read_csv(self, lines, path: str) -> Generator[tuple, None, None]:
        delimiter = str(self.options.get("delimiter") or ",")
        quotechar = str(self.options.get("quotechar") or '"')
        header = self.options.get("header", True)
        reader = csv.reader(lines, delimiter=delimiter, quotechar=quotechar)

        if header:
            header_row = next(reader, None)
            if header_row is None:
                return
            col_index = self._map_header(header_row, path)
            expected = len(header_row)
        else:
            col_index = self._ordinal_index()
            expected = None

        for record in reader:
            lineno = reader.line_num
            if expected is not None and len(record) != expected:
                raise FileSourceError(
                    f"CSV bozuk satir: {path} satir {lineno}: "
                    f"{len(record)} alan, header {expected} bekliyordu."
                )
            try:
                yield tuple(record[i] for i in col_index)
            except IndexError as exc:
                raise FileSourceError(
                    f"CSV bozuk satir: {path} satir {lineno}: "
                    "kolon indeksi satir uzunlugunu asti."
                ) from exc

    def _read_json_flat(self, lines, path: str) -> Generator[tuple, None, None]:
        for lineno, raw in enumerate(lines, start=1):
            stripped = raw.strip()
            if not stripped:
                continue
            try:
                obj = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise FileSourceError(
                    f"JSON flat parse hatasi: {path} satir {lineno}: {exc.msg}. "
                    "flat mod her satirda tek bir JSON obje (JSONL) bekler."
                ) from exc
            if not isinstance(obj, dict):
                raise FileSourceError(
                    f"JSON flat: {path} satir {lineno} bir JSON obje degil "
                    f"({type(obj).__name__}); flat mod obje bekler."
                )
            yield self._json_row(obj, path, lineno)

    def _json_row(self, obj: dict, path: str, lineno: int) -> tuple:
        row = []
        for col in self.source_columns:
            val = obj.get(col)
            if isinstance(val, (dict, list)):
                raise FileSourceError(
                    f"JSON flat: {path} satir {lineno} '{col}' alani nested "
                    f"({type(val).__name__}); flat mod skaler bekler "
                    "(nested → raw modu, F1.4b)."
                )
            row.append(val)
        return tuple(row)

    # ------------------------------------------------------------------
    # Column mapping
    # ------------------------------------------------------------------

    def _map_header(self, header_row: list[str], path: str) -> list[int]:
        pos = {name: i for i, name in enumerate(header_row)}
        idx: list[int] = []
        for col in self.source_columns:
            if col not in pos:
                raise FileSourceError(
                    f"CSV header'da '{col}' kolonu yok: {path}. "
                    f"Header: {header_row}"
                )
            idx.append(pos[col])
        return idx

    def _ordinal_index(self) -> list[int]:
        idx: list[int] = []
        for col in self.source_columns:
            try:
                n = int(str(col))
            except ValueError as exc:
                raise FileSourceError(
                    "header=false icin source_name 1-tabanli ordinal olmali; "
                    f"gecersiz: '{col}'."
                ) from exc
            if n < 1:
                raise FileSourceError(f"Ordinal 1'den kucuk olamaz: '{col}'.")
            idx.append(n - 1)
        return idx


def _decode_lines(stream, encoding: str):
    """Yield decoded text lines from a binary stream (constant RAM)."""
    for raw in stream:
        yield raw.decode(encoding)
