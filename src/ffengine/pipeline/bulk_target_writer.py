"""
F2.1 — BulkTargetWriter: native bulk write path (Enterprise providers).

Selected by ``build_target_writer`` when ``use_bulk_api=True``. Community ships
**no** providers, so any ``use_bulk_api=True`` fails loud here
(developer-controlled; **no** silent fallback to ``executemany``). Stateful
lifecycle matching the Streamer / W5 contract:

    prepare(config)               -> validate + open provider/staging
    write_batch(rows, config) * N -> stream to staging (COPY / direct-path)
    finalize()                    -> promote staging -> target (per load_method) + cleanup
    rollback_batch()              -> abort + cleanup on failure

``use_bulk_api=False`` never reaches here (``build_target_writer`` returns the
unchanged ``TargetWriter``).

Provider protocol (implemented by Enterprise/customer packages and registered via
``ffengine.pipeline.bulk_registry``):

    class Provider:
        MAX_WRITERS: int                       # e.g. PG COPY = 1, Oracle direct-path = 1
        DEFAULT_WRITERS: int                   # capability default when writer_workers is auto (None)
        SUPPORTED_LOAD_METHODS: frozenset[str]
        def __init__(self, session, dialect, task_config, effective_writers): ...
        def open(self) -> None: ...
        def write_batch(self, rows, task_config) -> int: ...
        def finalize(self) -> None: ...        # promote + cleanup
        def abort(self) -> None: ...           # cleanup on failure
"""

from __future__ import annotations

from ffengine.errors.exceptions import ConfigError
from ffengine.pipeline.bulk_registry import get_bulk_provider, valid_methods_for


def conn_type_of(dialect) -> str:
    """Canonical bulk-registry key for a dialect instance (e.g. 'postgres')."""
    return type(dialect).__name__.lower().replace("dialect", "")


def resolve_effective_writers(
    task_config: dict, *, max_writers: int, default_writers: int, method: str
) -> int:
    """Effective writer count (M) for a bulk provider (review §8 / D-G).

    ``writer_workers`` is ``None`` (auto default) -> provider capability default.
    An explicit integer is a developer override; ``M > max_writers`` is fail-loud
    (e.g. Oracle direct-path exclusive lock -> M=1). The legacy config default is
    never treated as an explicit override (schema default is ``None``).
    """
    explicit = task_config.get("writer_workers")
    if explicit is None:
        return max(1, int(default_writers))
    if isinstance(explicit, bool) or not isinstance(explicit, int) or explicit < 1:
        raise ConfigError(
            f"writer_workers >= 1 tam sayi olmali (bulk method '{method}'), "
            f"su an: {explicit!r}."
        )
    if explicit > max_writers:
        raise ConfigError(
            f"bulk_api_method='{method}': writer_workers={explicit} desteklenmiyor "
            f"(maksimum M={max_writers}). writer_workers'i {max_writers} yapin "
            "veya bos (auto) birakin."
        )
    return explicit


def build_bulk_target_writer(session, dialect, task_config: dict):
    """Fail-loud capability probe -> BulkTargetWriter for a registered provider."""
    method = str(task_config.get("bulk_api_method") or "").strip().lower()
    if not method:
        raise ConfigError(
            "use_bulk_api=True icin bulk_api_method zorunludur (explicit; 'auto' yok)."
        )
    conn_type = conn_type_of(dialect)
    provider_cls = get_bulk_provider(conn_type, method)
    if provider_cls is None:
        available = sorted(valid_methods_for(conn_type))
        raise ConfigError(
            f"Bulk API desteklenmiyor: conn_type={conn_type!r}, method={method!r}. "
            f"Bu conn_type icin kayitli yontemler: {available or '(hicbiri)'}. "
            "Sessizce executemany'e dusulmez (fail-loud)."
        )
    return BulkTargetWriter(session, dialect, provider_cls, method)


class BulkTargetWriter:
    """Community orchestrator; driver-specific work lives in the Enterprise provider."""

    def __init__(self, session, dialect, provider_cls, method):
        self.session = session
        self.dialect = dialect
        self._provider_cls = provider_cls
        self._method = method
        self._provider = None

    def prepare(self, task_config: dict) -> None:
        cls = self._provider_cls
        max_writers = int(getattr(cls, "MAX_WRITERS", 1))
        default_writers = int(getattr(cls, "DEFAULT_WRITERS", 1))
        supported = getattr(cls, "SUPPORTED_LOAD_METHODS", None)

        load_method = str(task_config.get("load_method") or "append")
        if supported is not None and load_method not in supported:
            raise ConfigError(
                f"bulk_api_method='{self._method}' load_method='{load_method}' "
                f"desteklemiyor. Desteklenen: {sorted(supported)}. "
                "Kayit oncesi reddedilir (sessiz executemany yok)."
            )
        effective_writers = resolve_effective_writers(
            task_config,
            max_writers=max_writers,
            default_writers=default_writers,
            method=self._method,
        )
        self._provider = cls(self.session, self.dialect, task_config, effective_writers)
        self._provider.open()

    def write_batch(self, rows, task_config: dict) -> int:
        if not rows:
            return 0
        if self._provider is None:
            raise ConfigError("BulkTargetWriter.prepare() cagrilmadan write_batch.")
        return int(self._provider.write_batch(rows, task_config))

    def finalize(self) -> None:
        if self._provider is not None:
            self._provider.finalize()

    def rollback_batch(self) -> None:
        if self._provider is not None:
            self._provider.abort()
