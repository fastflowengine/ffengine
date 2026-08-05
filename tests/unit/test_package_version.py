"""Package metadata and runtime version must move together."""

from pathlib import Path
import tomllib

import ffengine


def test_runtime_version_matches_pyproject() -> None:
    project_root = Path(__file__).resolve().parents[2]
    metadata = tomllib.loads(
        (project_root / "pyproject.toml").read_text(encoding="utf-8")
    )

    assert ffengine.__version__ == metadata["project"]["version"]
