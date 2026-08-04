"""
FFEngine Core Layer
BaseEngine, FlowResult, FlowManager / StandardEngine (PythonEngine alias).
"""

from ffengine.core.base_engine import BaseEngine, FlowResult
from ffengine.core.flow_manager import FlowManager, PythonEngine, StandardEngine

__all__ = [
    "BaseEngine",
    "FlowResult",
    "FlowManager",
    "PythonEngine",
    "StandardEngine",
]
