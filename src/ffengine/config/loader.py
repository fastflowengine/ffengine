"""
C05 — YAML config yükleyici.

ConfigLoader.load(config_path, task_group_id) → normalize edilmiş task dict.
"""

import copy
from pathlib import Path

import yaml

from ffengine.config.schema import (
    ENGINE_BLOCK_FIELD,
    ENGINE_PREFERENCE_FIELD,
    ENGINE_PREFERENCE_KEY,
    ENGINE_SPARK_FIELD,
    ENGINE_SPARK_KEY,
    REQUIRED_ROOT_FIELDS,
    TASK_DEFAULTS,
    VALID_ENGINE_BLOCK_FIELDS,
    VALID_ENGINE_SPARK_FIELDS,
)
from ffengine.config.validator import ConfigValidator
from ffengine.errors.exceptions import ConfigError


class ConfigLoader:
    """
    YAML config dosyasını yükler, task'ı bulur, varsayılan değerleri
    uygular ve doğrulamasını çalıştırır.

    Kullanım::

        task_config = ConfigLoader().load("path/to/config.yaml", "my_task")

    Dönen dict, FlowManager.run_flow_task() için doğrudan kullanılabilir.
    """

    def load(self, config_path: str, task_group_id: str) -> dict:
        """
        Parameters
        ----------
        config_path   : YAML dosyasının yolu.
        task_group_id : Çalıştırılacak task'ın kimliği.

        Returns
        -------
        Normalize edilmiş ve doğrulanmış task config dict'i.

        Raises
        ------
        ConfigError      : Dosya bulunamadı, YAML parse hatası, zorunlu alan eksik.
        ValidationError  : Whitelist veya koşullu kural ihlali.
        """
        raw = self._read_yaml(config_path)
        self._validate_root(raw)
        task = self._find_task(raw["flow_tasks"], task_group_id)
        self._reject_reserved_task_fields(task)
        normalized = self._apply_defaults(task)
        self._resolve_mapping_file_path(normalized, config_path)
        self._attach_engine_preference(raw, normalized)
        ConfigValidator().validate(normalized)
        return normalized

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _read_yaml(self, config_path: str) -> dict:
        try:
            with open(config_path, "r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh)
        except FileNotFoundError as exc:
            raise ConfigError(f"Config dosyası bulunamadı: '{config_path}'") from exc
        except yaml.YAMLError as exc:
            raise ConfigError(f"YAML parse hatası '{config_path}': {exc}") from exc
        if not isinstance(data, dict):
            raise ConfigError(
                f"Config dosyası geçerli bir YAML mapping değil: '{config_path}'"
            )
        return data

    def _validate_root(self, raw: dict) -> None:
        for field in REQUIRED_ROOT_FIELDS:
            if field not in raw or raw[field] is None:
                raise ConfigError(f"Root alanı eksik veya boş: '{field}'")

    def _find_task(self, flow_tasks: list, task_group_id: str) -> dict:
        if not isinstance(flow_tasks, list):
            raise ConfigError("'flow_tasks' bir liste olmalıdır.")
        for task in flow_tasks:
            if isinstance(task, dict) and task.get("task_group_id") == task_group_id:
                return task
        raise ConfigError(f"task_group_id '{task_group_id}' config'te bulunamadı.")

    def _apply_defaults(self, task: dict) -> dict:
        result = copy.deepcopy(TASK_DEFAULTS)
        result.update(task)
        # Partitioning: sadece task'ta varsa default'u güncelle
        if "partitioning" in task and isinstance(task["partitioning"], dict):
            merged = copy.deepcopy(TASK_DEFAULTS["partitioning"])
            merged.update(task["partitioning"])
            result["partitioning"] = merged
        return result

    def _reject_reserved_task_fields(self, task: dict) -> None:
        reserved = (ENGINE_PREFERENCE_KEY, ENGINE_SPARK_KEY)
        for field in reserved:
            if field in task:
                raise ConfigError(
                    f"Task field '{field}' is reserved for internal use. "
                    "Configure motor selection only through root engine.preference "
                    "and engine.spark."
                )

    def _attach_engine_preference(self, raw: dict, task: dict) -> None:
        """F6.0 — kök ``engine:`` bloğunu task runtime dict'ine taşır.

        Blok **şekli** burada doğrulanır (kök alan → ``ConfigError``, mevcut
        ``_validate_root`` deseniyle tutarlı). **Değer** doğrulaması
        ``ConfigValidator._check_engine``'e aittir (``ValidationError``/422) —
        validator yalnız task dict'ini gördüğü için taşıma zorunludur.
        """
        block = raw.get(ENGINE_BLOCK_FIELD)
        if block is None:
            return
        if not isinstance(block, dict):
            raise ConfigError(
                f"Root alanı '{ENGINE_BLOCK_FIELD}' bir mapping olmalıdır; "
                f"ör. {ENGINE_BLOCK_FIELD}: {{{ENGINE_PREFERENCE_FIELD}: auto}}"
            )
        unknown = sorted(set(block) - VALID_ENGINE_BLOCK_FIELDS)
        if unknown:
            raise ConfigError(
                f"'{ENGINE_BLOCK_FIELD}' bloğunda bilinmeyen alan(lar): "
                f"{unknown}. Geçerli alan(lar): "
                f"{sorted(VALID_ENGINE_BLOCK_FIELDS)}."
            )
        if ENGINE_PREFERENCE_FIELD in block:
            task[ENGINE_PREFERENCE_KEY] = block[ENGINE_PREFERENCE_FIELD]
        if ENGINE_SPARK_FIELD in block:
            spark = block[ENGINE_SPARK_FIELD]
            if not isinstance(spark, dict):
                raise ConfigError("Root field 'engine.spark' must be a mapping.")
            unknown_spark = sorted(set(spark) - VALID_ENGINE_SPARK_FIELDS)
            if unknown_spark:
                raise ConfigError(
                    "'engine.spark' contains unknown field(s): "
                    f"{unknown_spark}. Valid fields: "
                    f"{sorted(VALID_ENGINE_SPARK_FIELDS)}."
                )
            task[ENGINE_SPARK_KEY] = copy.deepcopy(spark)

    def _resolve_mapping_file_path(self, task: dict, config_path: str) -> None:
        """mapping_file relatif ise config dosyasina gore absolute cozumler."""
        if str(task.get("column_mapping_mode") or "source") != "mapping_file":
            return
        mapping_file = str(task.get("mapping_file") or "").strip()
        if not mapping_file:
            return
        p = Path(mapping_file)
        if p.is_absolute():
            return
        task["mapping_file"] = str((Path(config_path).resolve().parent / p).resolve())
