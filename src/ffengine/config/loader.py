"""
C05 — YAML config yükleyici.

ConfigLoader.load(config_path, task_group_id) → normalize edilmiş task dict.
"""

import copy
from pathlib import Path

import yaml

from ffengine.config.schema import REQUIRED_ROOT_FIELDS, TASK_DEFAULTS
from ffengine.config.validator import ConfigValidator
from ffengine.errors.exceptions import ConfigError


class ConfigLoader:
    """
    YAML config dosyasını yükler, task'ı bulur, varsayılan değerleri
    uygular ve doğrulamasını çalıştırır.

    Kullanım::

        task_config = ConfigLoader().load("path/to/config.yaml", "my_task")

    Dönen dict, ETLManager.run_etl_task() için doğrudan kullanılabilir.
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
        task = self._find_task(raw["etl_tasks"], task_group_id)
        normalized = self._apply_defaults(task)
        self._resolve_mapping_file_path(normalized, config_path)
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
            raise ConfigError(
                f"Config dosyası bulunamadı: '{config_path}'"
            ) from exc
        except yaml.YAMLError as exc:
            raise ConfigError(
                f"YAML parse hatası '{config_path}': {exc}"
            ) from exc
        if not isinstance(data, dict):
            raise ConfigError(
                f"Config dosyası geçerli bir YAML mapping değil: '{config_path}'"
            )
        return data

    def _validate_root(self, raw: dict) -> None:
        for field in REQUIRED_ROOT_FIELDS:
            if field not in raw or raw[field] is None:
                raise ConfigError(f"Root alanı eksik veya boş: '{field}'")

    def _find_task(self, etl_tasks: list, task_group_id: str) -> dict:
        if not isinstance(etl_tasks, list):
            raise ConfigError("'etl_tasks' bir liste olmalıdır.")
        for task in etl_tasks:
            if isinstance(task, dict) and task.get("task_group_id") == task_group_id:
                return task
        raise ConfigError(
            f"task_group_id '{task_group_id}' config'te bulunamadı."
        )

    def _apply_defaults(self, task: dict) -> dict:
        result = copy.deepcopy(TASK_DEFAULTS)
        result.update(task)
        # Partitioning: sadece task'ta varsa default'u güncelle
        if "partitioning" in task and isinstance(task["partitioning"], dict):
            merged = copy.deepcopy(TASK_DEFAULTS["partitioning"])
            merged.update(task["partitioning"])
            result["partitioning"] = merged
        return result

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
