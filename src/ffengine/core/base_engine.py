"""
BaseEngine ABC ve ETLResult dataclass — Community + Enterprise ortak kontrat.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class ETLResult:
    rows: int
    duration_seconds: float
    throughput: float          # rows / second
    partitions_completed: int
    errors: list[str] = field(default_factory=list)


class BaseEngine(ABC):
    @abstractmethod
    def run(self, config_path: str, task_group_id: str) -> ETLResult: ...

    @abstractmethod
    def is_available(self) -> bool: ...

    @classmethod
    def detect(cls, preference: str = "auto") -> "BaseEngine":
        """
        Engine seçimi:
        - 'community' → PythonEngine
        - 'enterprise' → CEngine (E01'de implement edilecek)
        - 'auto'       → CEngine varsa CEngine, yoksa PythonEngine

        C04 scope'unda yalnızca PythonEngine döner.
        """
        from ffengine.core.etl_manager import PythonEngine

        if preference == "community":
            return PythonEngine()

        # Enterprise engine yoksa community'e düş
        try:
            if preference in ("enterprise", "auto"):
                from ffengine.enterprise.engine import CEngine  # type: ignore
                engine = CEngine()
                if engine.is_available():
                    return engine
        except ImportError:
            pass

        return PythonEngine()
