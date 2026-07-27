"""
F1.4 — FileSourceReader unit tests (CSV + JSON flat).

Covers: constant-RAM chunking (T-F1.4-1), corrupt row fail-loud with line no
(T-F1.4-2), JSON flat obj→row + nested fail-loud (T-F1.4-4), header/ordinal
mapping, and glob over multiple files. Local `fs` transport (runs everywhere).
"""

import pytest

from ffengine.errors.exceptions import FileSourceError
from ffengine.pipeline.file_source_reader import FileSourceReader
from ffengine.pipeline.file_transport import FileSourceContext


def _ctx(path, source_type="csv", **options):
    return FileSourceContext(
        conn_id="fs_default",
        conn_type="fs",
        file_path=str(path),
        source_type=source_type,
        options=options,
    )


def _reader(path, columns, source_type="csv", batch_size=10_000, **options):
    ctx = _ctx(path, source_type=source_type, **options)
    return FileSourceReader(
        ctx, {"source_columns": columns, "batch_size": batch_size}
    )


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------


class TestCsv:
    def test_reads_rows_in_source_column_order(self, tmp_path):
        p = tmp_path / "orders.csv"
        p.write_text("name,id\nalice,1\nbob,2\n", encoding="utf-8")
        # source_columns order differs from header order → reorder by name.
        rows = list(_reader(p, ["id", "name"]).read())
        assert rows == [[("1", "alice"), ("2", "bob")]]

    def test_constant_ram_chunking(self, tmp_path):
        p = tmp_path / "big.csv"
        p.write_text("id\n" + "\n".join(str(i) for i in range(5)) + "\n", "utf-8")
        chunks = list(_reader(p, ["id"], batch_size=2).read())
        assert [len(c) for c in chunks] == [2, 2, 1]
        assert sum(len(c) for c in chunks) == 5

    def test_custom_delimiter(self, tmp_path):
        p = tmp_path / "semi.csv"
        p.write_text("id;name\n1;alice\n", encoding="utf-8")
        rows = list(_reader(p, ["id", "name"], delimiter=";").read())
        assert rows == [[("1", "alice")]]

    def test_corrupt_row_fails_loud_with_line_number(self, tmp_path):
        p = tmp_path / "bad.csv"
        p.write_text("id,name\n1,alice\n2,bob,EXTRA\n", encoding="utf-8")
        with pytest.raises(FileSourceError, match="satir 3"):
            list(_reader(p, ["id", "name"]).read())

    def test_missing_header_column_fails_loud(self, tmp_path):
        p = tmp_path / "h.csv"
        p.write_text("id,name\n1,alice\n", encoding="utf-8")
        with pytest.raises(FileSourceError, match="zip"):
            list(_reader(p, ["id", "zip"]).read())

    def test_headerless_ordinal_mapping(self, tmp_path):
        p = tmp_path / "noh.csv"
        p.write_text("alice,10\nbob,20\n", encoding="utf-8")
        rows = list(_reader(p, ["1", "2"], header=False).read())
        assert rows == [[("alice", "10"), ("bob", "20")]]

    def test_glob_reads_multiple_files_sorted(self, tmp_path):
        (tmp_path / "orders_1.csv").write_text("id\n1\n", "utf-8")
        (tmp_path / "orders_2.csv").write_text("id\n2\n", "utf-8")
        pattern = tmp_path / "orders_*.csv"
        rows = list(_reader(pattern, ["id"]).read())
        assert rows == [[("1",), ("2",)]]


# ---------------------------------------------------------------------------
# JSON flat (JSONL)
# ---------------------------------------------------------------------------


class TestJsonFlat:
    def test_object_per_line_to_rows(self, tmp_path):
        p = tmp_path / "d.jsonl"
        p.write_text('{"id":1,"name":"a"}\n{"id":2,"name":"b"}\n', "utf-8")
        rows = list(_reader(p, ["id", "name"], source_type="json").read())
        assert rows == [[(1, "a"), (2, "b")]]

    def test_nested_value_in_flat_fails_loud(self, tmp_path):
        p = tmp_path / "nested.jsonl"
        p.write_text('{"id":1,"info":{"x":1}}\n', "utf-8")
        with pytest.raises(FileSourceError, match="nested"):
            list(_reader(p, ["id", "info"], source_type="json").read())

    def test_non_object_line_fails_loud(self, tmp_path):
        p = tmp_path / "arr.jsonl"
        p.write_text("[1, 2, 3]\n", encoding="utf-8")
        with pytest.raises(FileSourceError, match="obje degil"):
            list(_reader(p, ["id"], source_type="json").read())

    def test_blank_lines_skipped(self, tmp_path):
        p = tmp_path / "gap.jsonl"
        p.write_text('{"id":1}\n\n{"id":2}\n', encoding="utf-8")
        rows = list(_reader(p, ["id"], source_type="json").read())
        assert rows == [[(1,), (2,)]]


def test_empty_source_columns_fails_loud(tmp_path):
    p = tmp_path / "x.csv"
    p.write_text("id\n1\n", encoding="utf-8")
    ctx = _ctx(p)
    reader = FileSourceReader(ctx, {"source_columns": []})
    with pytest.raises(FileSourceError, match="source_columns"):
        list(reader.read())
