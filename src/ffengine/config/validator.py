"""
C05 — Config doğrulama kuralları.

ConfigValidator.validate(task) geçersiz config için
ffengine.errors.exceptions.ConfigError veya ValidationError fırlatır.
"""

import logging

from ffengine.errors.exceptions import ConfigError, ValidationError
from ffengine.config.schema import (
    REQUIRED_TASK_FIELDS,
    VALID_SOURCE_TYPES,
    VALID_LOAD_METHODS,
    VALID_COLUMN_MAPPING_MODES,
    VALID_EXTRACTION_METHODS,
    VALID_PARTITION_MODES,
)

_log = logging.getLogger(__name__)


class ConfigValidator:
    """
    Task config dict'ini doğrular.

    Kontroller sırasıyla:
      1. Zorunlu alan varlığı
      2. source_type whitelist
      3. load_method whitelist
      4. column_mapping_mode whitelist
      5. extraction_method whitelist
      6. source_type=sql → sql_file veya inline_sql zorunlu
      7. column_mapping_mode=mapping_file → mapping_file yolu zorunlu
      8. partitioning kuralları (C06)
      9. passthrough_full=False → source_columns zorunlu (C09)
    """

    def validate(self, task: dict) -> None:
        self._check_required(task)
        self._check_source_type(task)
        self._check_load_method(task)
        self._check_column_mapping_mode(task)
        self._check_extraction_method(task)
        self._check_sql_source(task)
        self._check_mapping_file(task)
        self._check_partitioning(task)
        self._check_passthrough_config(task)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _check_required(self, task: dict) -> None:
        for field in REQUIRED_TASK_FIELDS:
            if task.get(field) is None:
                raise ConfigError(f"Zorunlu alan eksik: '{field}'")

    def _check_source_type(self, task: dict) -> None:
        value = task.get("source_type")
        if value not in VALID_SOURCE_TYPES:
            raise ValidationError(
                f"Geçersiz source_type: '{value}'. "
                f"Geçerli değerler: {sorted(VALID_SOURCE_TYPES)}"
            )

    def _check_load_method(self, task: dict) -> None:
        value = task.get("load_method")
        if value not in VALID_LOAD_METHODS:
            raise ValidationError(
                f"Geçersiz load_method: '{value}'. "
                f"Geçerli değerler: {sorted(VALID_LOAD_METHODS)}"
            )

    def _check_column_mapping_mode(self, task: dict) -> None:
        value = task.get("column_mapping_mode", "source")
        if value not in VALID_COLUMN_MAPPING_MODES:
            raise ValidationError(
                f"Geçersiz column_mapping_mode: '{value}'. "
                f"Geçerli değerler: {sorted(VALID_COLUMN_MAPPING_MODES)}"
            )

    def _check_extraction_method(self, task: dict) -> None:
        value = task.get("extraction_method", "auto")
        if value not in VALID_EXTRACTION_METHODS:
            raise ValidationError(
                f"Geçersiz extraction_method: '{value}'. "
                f"Geçerli değerler: {sorted(VALID_EXTRACTION_METHODS)}"
            )
        if value == "copy_binary":
            _log.warning(
                "extraction_method=copy_binary Community modunda etkin değildir; "
                "cursor modu kullanılacak."
            )

    def _check_sql_source(self, task: dict) -> None:
        if task.get("source_type") == "sql":
            has_sql = task.get("sql_file") or task.get("inline_sql")
            if not has_sql:
                raise ValidationError(
                    "source_type='sql' için 'sql_file' veya 'inline_sql' zorunludur."
                )

    def _check_mapping_file(self, task: dict) -> None:
        if task.get("column_mapping_mode") == "mapping_file":
            if not task.get("mapping_file"):
                raise ValidationError(
                    "column_mapping_mode='mapping_file' için 'mapping_file' yolu zorunludur."
                )

    def _check_partitioning(self, task: dict) -> None:
        part = task.get("partitioning")
        if not isinstance(part, dict) or not part.get("enabled"):
            return

        # mode whitelist (normalize "auto" → "auto_numeric" in-place)
        mode = part.get("mode", "auto")
        if mode == "auto":
            mode = "auto_numeric"
            part["mode"] = "auto_numeric"
        if mode not in VALID_PARTITION_MODES:
            raise ValidationError(
                f"Geçersiz partitioning.mode: '{mode}'. "
                f"Geçerli değerler: {sorted(VALID_PARTITION_MODES)}"
            )

        # parts >= 1
        parts = part.get("parts", 4)
        if not isinstance(parts, int) or parts < 1:
            raise ValidationError(
                f"partitioning.parts >= 1 olmalıdır, şu an: {parts!r}"
            )

        # column-dependent modes require column
        _COLUMN_MODES = {"auto_numeric", "percentile", "hash_mod", "distinct"}
        if mode in _COLUMN_MODES and not part.get("column"):
            raise ValidationError(
                f"partitioning.mode='{mode}' için 'partitioning.column' zorunludur."
            )

        # explicit mode requires non-empty ranges
        if mode == "explicit" and not part.get("ranges"):
            raise ValidationError(
                "partitioning.mode='explicit' için 'partitioning.ranges' listesi boş olamaz."
            )

    def _check_passthrough_config(self, task: dict) -> None:
        """passthrough_full=False ise source_columns zorunludur."""
        if task.get("column_mapping_mode", "source") != "source":
            return  # mapping_file modunda passthrough_full geçerli değil
        if task.get("passthrough_full", True) is False:
            if not task.get("source_columns"):
                raise ValidationError(
                    "passthrough_full=False iken 'source_columns' listesi zorunludur."
                )
