"""
FFEngine Core Layer
BaseEngine, ETLResult, ETLManager / PythonEngine.
"""

from ffengine.core.base_engine import BaseEngine, ETLResult
from ffengine.core.etl_manager import ETLManager, PythonEngine

__all__ = [
    "BaseEngine",
    "ETLResult",
    "ETLManager",
    "PythonEngine",
]
