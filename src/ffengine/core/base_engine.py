"""
BaseEngine ABC ve FlowResult dataclass — Community + Enterprise ortak kontrat.

F2.3: engine seçimi **capability-based**tir (DECISIONS "F2.3 engine
detection"). Community hiçbir Enterprise paketini import etmez/adlandırmaz;
Enterprise motorları ``ffengine.core.engine_registry`` üzerinden kayıt olur.
"""

from __future__ import annotations

import logging
import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

log = logging.getLogger(__name__)

_CANONICAL_PREFERENCES = ("auto", "standard", "pipeline", "spark")

# Legacy aliases keep working but warn: 'community' was always the
# standard engine; 'enterprise' used to mean "use the Enterprise engine
# if present, else fall back" — i.e. today's 'auto'.
_LEGACY_ALIASES = {"community": "standard", "enterprise": "auto"}


@dataclass
class FlowResult:
    rows: int
    duration_seconds: float
    throughput: float  # rows / second
    partitions_completed: int
    errors: list[str] = field(default_factory=list)
    # F3.3 K1 (additive) — aktarım muhasebesi. Legacy çağrılar bu alanları
    # vermez; __post_init__ onları `rows` üzerinden türetir ve durumu
    # "legacy" işaretler, böylece eski davranış bire bir korunur.
    rows_read: int | None = None
    rows_written: int | None = None
    rows_rejected: int = 0
    reconciliation_status: str = "legacy"

    def __post_init__(self) -> None:
        if self.rows_read is None:
            self.rows_read = self.rows
        if self.rows_written is None:
            self.rows_written = self.rows


class BaseEngine(ABC):
    @abstractmethod
    def run(self, config_path: str, task_group_id: str) -> FlowResult: ...

    @abstractmethod
    def is_available(self) -> bool: ...

    @classmethod
    def detect(cls, preference: str = "auto") -> "BaseEngine":
        """
        Capability-based engine seçimi:
        - 'standard'        → StandardEngine
        - 'auto'            → kayıtlı+available 'pipeline' provider varsa o,
                              yoksa StandardEngine
        - 'pipeline'/'spark' → kayıtlı provider yoksa **fail-loud EngineError**
                              (sessiz fallback YOK); kayıtlı ama unavailable
                              (edition/lisans) ise yine fail-loud
        - legacy 'community'/'enterprise' alias'ları DeprecationWarning üretir
        """
        from ffengine.core import engine_registry
        from ffengine.core.flow_manager import StandardEngine
        from ffengine.errors.exceptions import EngineError

        raw = str(preference or "auto").strip().lower()
        if raw in _LEGACY_ALIASES:
            resolved = _LEGACY_ALIASES[raw]
            warnings.warn(
                f"Engine preference '{raw}' is a legacy alias; use "
                f"'{resolved}' instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            log.warning(
                "Legacy engine preference %r used; resolved to %r.",
                raw,
                resolved,
            )
            raw = resolved
        if raw not in _CANONICAL_PREFERENCES:
            raise EngineError(
                f"Unknown engine preference '{raw}'; expected one of: "
                f"{', '.join(_CANONICAL_PREFERENCES)}."
            )

        if raw == "standard":
            return StandardEngine()

        if raw == "auto":
            provider = engine_registry.get_engine_provider("pipeline")
            if provider is not None:
                engine = provider()
                if engine.is_available():
                    return engine
            return StandardEngine()

        provider = engine_registry.get_engine_provider(raw)
        if provider is None:
            raise EngineError(
                f"Engine '{raw}' requires an Enterprise engine provider, "
                "but none is registered (entry point group "
                f"'{engine_registry.ENTRY_POINT_GROUP}'). Install/enable "
                "FFEngine Enterprise or choose 'standard'/'auto'."
            )
        engine = provider()
        if not engine.is_available():
            raise EngineError(
                f"Engine provider '{raw}' is registered but reports "
                "unavailable — check the Enterprise edition/license state."
            )
        return engine
