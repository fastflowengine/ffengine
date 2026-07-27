"""
F1.5 — File transport layer (WHERE the bytes live).

Transport ⟂ format: this module only opens byte streams for a file endpoint
resolved from an Airflow Connection (local ``fs`` / ``sftp``). It never parses
content (that is the F1.4 reader) and never holds credentials — they live only
in the Airflow Connection (INV-5). SFTP reads/writes stream directly (no local
download); the target promotes an atomic temp→rename.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, BinaryIO

from ffengine.errors.exceptions import ConfigError, FileTransportError

_log = logging.getLogger(__name__)

_FS_CONN_TYPES = frozenset({"fs", "file", "filesystem", ""})
_SFTP_CONN_TYPES = frozenset({"sftp", "ssh"})


def resolve_transport_kind(conn_type: str | None) -> str:
    """Map an Airflow ``conn_type`` to a file transport kind (``fs``/``sftp``)."""
    ct = str(conn_type or "").strip().lower()
    if ct in _SFTP_CONN_TYPES:
        return "sftp"
    if ct in _FS_CONN_TYPES:
        return "fs"
    raise ConfigError(
        f"Desteklenmeyen dosya transport conn_type: '{conn_type}'. "
        "Yalniz local (fs) ve sftp desteklenir."
    )


# ---------------------------------------------------------------------------
# Endpoint descriptors (passed to FlowManager in place of a DBSession)
# ---------------------------------------------------------------------------


@dataclass
class FileSourceContext:
    """A file SOURCE endpoint: transport + path + format options."""

    conn_id: str
    conn_type: str
    file_path: str
    source_type: str            # "csv" | "json"
    options: dict[str, Any] = field(default_factory=dict)


@dataclass
class FileTargetContext:
    """A file TARGET endpoint: transport + path + output options."""

    conn_id: str
    conn_type: str
    file_path: str
    options: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Stream handles (keep the client/hook alive while the stream is used)
# ---------------------------------------------------------------------------


class TransportReadHandle:
    """A binary read stream plus the resources that must outlive it."""

    def __init__(self, stream: BinaryIO, kind: str, *, client=None, hook=None):
        self.stream = stream
        self._kind = kind
        self._client = client
        self._hook = hook  # kept only as a keepalive reference

    def close(self) -> None:
        try:
            self.stream.close()
        finally:
            _safe_close(self._client)


class TransportWriteHandle:
    """A binary write stream with an atomic temp→final promotion."""

    def __init__(self, stream, kind, tmp_path, final_path, *, client=None, hook=None):
        self.stream = stream
        self._kind = kind
        self._tmp = tmp_path
        self._final = final_path
        self._client = client
        self._hook = hook
        self._stream_closed = False

    def _close_stream(self) -> None:
        if not self._stream_closed:
            try:
                self.stream.close()
            finally:
                self._stream_closed = True

    def promote(self) -> None:
        """Atomically move the temp file onto the final path."""
        self._close_stream()
        try:
            if self._kind == "fs":
                os.replace(self._tmp, self._final)
            else:
                _sftp_promote(self._client, self._tmp, self._final)
        finally:
            _safe_close(self._client)

    def abort(self) -> None:
        """Drop the temp file; the final path is never touched."""
        self._close_stream()
        try:
            if self._kind == "fs":
                if os.path.exists(self._tmp):
                    os.remove(self._tmp)
            elif self._client is not None:
                self._client.remove(self._tmp)
        except Exception as exc:  # cleanup is best-effort
            _log.warning(
                "file_transport.abort_cleanup_failed: %s", type(exc).__name__
            )
        finally:
            _safe_close(self._client)


# ---------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------


def resolve_read_paths(conn_id: str, conn_type: str, pattern: str) -> list[str]:
    """Expand a (possibly glob) path into a sorted list of concrete paths."""
    kind = resolve_transport_kind(conn_type)
    if kind == "fs":
        return _fs_resolve_read(conn_id, pattern)
    return _sftp_resolve_read(conn_id, pattern)


def open_read(conn_id: str, conn_type: str, resolved_path: str) -> TransportReadHandle:
    """Open a resolved concrete path for streaming binary read."""
    kind = resolve_transport_kind(conn_type)
    if kind == "fs":
        return TransportReadHandle(open(resolved_path, "rb"), kind)
    hook, client = _sftp_connect(conn_id)
    stream = client.open(resolved_path, "rb")  # paramiko SFTPFile — streams
    return TransportReadHandle(stream, kind, client=client, hook=hook)


def _fs_resolve_read(conn_id: str, pattern: str) -> list[str]:
    import glob as _glob

    full = _fs_join(conn_id, pattern)
    matches = sorted(_glob.glob(full)) if _has_glob(pattern) else [full]
    if not matches:
        raise FileTransportError(f"Eslesen dosya yok: '{pattern}'")
    return matches


def _sftp_resolve_read(conn_id: str, pattern: str) -> list[str]:
    if not _has_glob(pattern):
        return [pattern]
    import fnmatch
    import posixpath

    hook, client = _sftp_connect(conn_id)
    try:
        directory = posixpath.dirname(pattern) or "."
        leaf = posixpath.basename(pattern)
        names = client.listdir(directory)
    finally:
        _safe_close(client)
    matches = sorted(
        posixpath.join(directory, n) for n in names if fnmatch.fnmatch(n, leaf)
    )
    if not matches:
        raise FileTransportError(f"Eslesen SFTP dosyasi yok: '{pattern}'")
    return matches


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------


def open_write(
    conn_id: str, conn_type: str, final_path: str, *, tmp_suffix: str
) -> TransportWriteHandle:
    """Open a temp file for write; promote() renames it onto ``final_path``."""
    kind = resolve_transport_kind(conn_type)
    if kind == "fs":
        final = _fs_write_path(conn_id, final_path)
        tmp = final + tmp_suffix
        parent = os.path.dirname(final)
        if parent:
            os.makedirs(parent, exist_ok=True)
        return TransportWriteHandle(open(tmp, "wb"), kind, tmp, final)
    tmp = final_path + tmp_suffix
    hook, client = _sftp_connect(conn_id)
    stream = client.open(tmp, "wb")
    return TransportWriteHandle(
        stream, kind, tmp, final_path, client=client, hook=hook
    )


def _fs_write_path(conn_id: str, path: str) -> str:
    return _fs_join(conn_id, path)


def _sftp_promote(client, tmp: str, final: str) -> None:
    try:
        client.posix_rename(tmp, final)  # atomic (openssh posix-rename ext)
    except (IOError, OSError):
        # Fallback for servers without posix-rename (best-effort, not atomic).
        try:
            client.remove(final)
        except IOError:
            pass
        client.rename(tmp, final)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _has_glob(path: str) -> bool:
    return any(ch in path for ch in "*?[")


def _fs_join(conn_id: str, path: str) -> str:
    """Join a relative path onto the FS connection's base; absolute wins."""
    if os.path.isabs(path):
        return path
    base = _fs_base_path(conn_id)
    return os.path.join(base, path) if base else path


def _fs_base_path(conn_id: str) -> str:
    try:
        from airflow.providers.standard.hooks.filesystem import FSHook

        base = FSHook(fs_conn_id=conn_id).get_path()
    except Exception:
        return ""
    return str(base or "").strip()


def _sftp_connect(conn_id: str):
    try:
        from airflow.providers.sftp.hooks.sftp import SFTPHook
    except ImportError as exc:
        raise FileTransportError(
            "SFTP transport icin 'apache-airflow-providers-sftp' gerekli."
        ) from exc
    hook = SFTPHook(ssh_conn_id=conn_id)
    return hook, hook.get_conn()


def _safe_close(obj) -> None:
    if obj is None:
        return
    try:
        obj.close()
    except Exception:
        pass
