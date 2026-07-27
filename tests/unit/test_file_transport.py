"""
F1.5 — file_transport unit tests (local fs round-trip + SFTP via a fake client).

Covers transport-kind resolution, local read/write/promote/abort + glob, and the
SFTP path (lazy hook) exercised through a monkeypatched ``_sftp_connect`` so no
real SFTP server or provider is needed. Credentials never appear here — the
transport only takes a conn_id and delegates to the hook (INV-5).
"""

import io
import os

import pytest

import ffengine.pipeline.file_transport as ft
from ffengine.errors.exceptions import ConfigError, FileTransportError


# ---------------------------------------------------------------------------
# transport-kind
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ct,kind", [("sftp", "sftp"), ("ssh", "sftp"),
                                     ("fs", "fs"), ("file", "fs"), ("", "fs")])
def test_resolve_transport_kind(ct, kind):
    assert ft.resolve_transport_kind(ct) == kind


def test_resolve_transport_kind_rejects_unknown():
    with pytest.raises(ConfigError, match="transport"):
        ft.resolve_transport_kind("s3")


# ---------------------------------------------------------------------------
# local fs
# ---------------------------------------------------------------------------


def test_fs_write_promote_read_roundtrip(tmp_path):
    final = tmp_path / "rt.csv"
    h = ft.open_write("fs_default", "fs", str(final), tmp_suffix=".tmp")
    h.stream.write(b"line1\nline2\n")
    assert not final.exists()  # still a temp
    h.promote()
    assert final.read_bytes() == b"line1\nline2\n"

    rh = ft.open_read("fs_default", "fs", str(final))
    try:
        assert rh.stream.read() == b"line1\nline2\n"
    finally:
        rh.close()


def test_fs_abort_removes_temp(tmp_path):
    final = tmp_path / "a.csv"
    h = ft.open_write("fs_default", "fs", str(final), tmp_suffix=".tmp")
    h.stream.write(b"x")
    h.abort()
    assert not final.exists()
    assert not (tmp_path / "a.csv.tmp").exists()


def test_fs_resolve_glob_sorted_and_single(tmp_path):
    (tmp_path / "a_1.csv").write_text("x", "utf-8")
    (tmp_path / "a_2.csv").write_text("y", "utf-8")
    got = ft.resolve_read_paths("fs_default", "fs", str(tmp_path / "a_*.csv"))
    assert [os.path.basename(p) for p in got] == ["a_1.csv", "a_2.csv"]
    one = ft.resolve_read_paths("fs_default", "fs", str(tmp_path / "a_1.csv"))
    assert len(one) == 1 and one[0].endswith("a_1.csv")


def test_fs_no_match_fails_loud(tmp_path):
    with pytest.raises(FileTransportError, match="Eslesen"):
        ft.resolve_read_paths("fs_default", "fs", str(tmp_path / "none_*.csv"))


# ---------------------------------------------------------------------------
# SFTP (fake client)
# ---------------------------------------------------------------------------


class _FakeSFTPClient:
    def __init__(self):
        self.files: dict = {}
        self.renamed: list = []
        self.removed: list = []
        self.dirs: dict = {}

    def open(self, path, mode):
        buf = io.BytesIO()
        buf.close = lambda: None  # keep bytes inspectable after close
        self.files[path] = buf
        return buf

    def posix_rename(self, src, dst):
        self.renamed.append((src, dst))

    def rename(self, src, dst):
        self.renamed.append(("plain", src, dst))

    def remove(self, path):
        self.removed.append(path)

    def listdir(self, directory):
        return self.dirs.get(directory, [])

    def close(self):
        pass


@pytest.fixture
def fake_sftp(monkeypatch):
    client = _FakeSFTPClient()
    monkeypatch.setattr(ft, "_sftp_connect", lambda conn_id: (object(), client))
    return client


def test_sftp_open_read_streams_no_download(fake_sftp):
    h = ft.open_read("sftp_x", "sftp", "/in/a.csv")
    assert "/in/a.csv" in fake_sftp.files  # opened remote handle directly
    h.close()


def test_sftp_write_promote_is_atomic_rename(fake_sftp):
    h = ft.open_write("sftp_x", "sftp", "/out/o.csv", tmp_suffix=".tmp")
    h.stream.write(b"data")
    h.promote()
    assert fake_sftp.renamed == [("/out/o.csv.tmp", "/out/o.csv")]


def test_sftp_abort_removes_temp(fake_sftp):
    h = ft.open_write("sftp_x", "sftp", "/out/o.csv", tmp_suffix=".tmp")
    h.abort()
    assert fake_sftp.removed == ["/out/o.csv.tmp"]


def test_sftp_resolve_glob_filters_directory(fake_sftp):
    fake_sftp.dirs["/in"] = ["orders_1.csv", "orders_2.csv", "other.txt"]
    paths = ft.resolve_read_paths("s", "sftp", "/in/orders_*.csv")
    assert paths == ["/in/orders_1.csv", "/in/orders_2.csv"]


def test_sftp_resolve_single_path_no_listdir(fake_sftp):
    paths = ft.resolve_read_paths("s", "sftp", "/in/exact.csv")
    assert paths == ["/in/exact.csv"]
