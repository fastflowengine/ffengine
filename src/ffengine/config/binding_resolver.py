"""
C05 - Binding cozumleme.

BindingResolver destekleri:
- Legacy template syntax: {{ source.X }}, {{ target.X }}, {{ literal.X }}, {{ airflow_var.KEY }}
- Yeni UI syntax: where clause icinde :param + bindings[] listesi
"""

from __future__ import annotations

import logging
import math
import re
from datetime import date, datetime
from typing import Any

from ffengine.errors.exceptions import ConfigError

_log = logging.getLogger(__name__)

# Legacy: {{ source.col }}, {{ literal.val }}, {{ airflow_var.KEY }}
_LEGACY_BINDING_RE = re.compile(r"\{\{\s*(source|target|literal|airflow_var)\.(\S+?)\s*\}\}")
_PARAM_RE = re.compile(r":([A-Za-z_][A-Za-z0-9_]*)")


class BindingResolver:
    """Task baglama/parametre cozumleyici."""

    def resolve(self, task_config: dict, context: dict | None = None) -> dict:
        """Legacy template bindinglerini cozer ve _resolved_where yazar."""
        ctx = context or {}
        result = dict(task_config)

        where = task_config.get("where")
        if where:
            result["_resolved_where"] = self._resolve_legacy_string(str(where), task_config, ctx)

        return result

    def resolve_sql_bindings(
        self,
        task_config: dict,
        *,
        context: dict | None,
        source_session: Any,
        target_session: Any,
    ) -> dict:
        """
        Yeni UI binding modelini cozer.

        Notlar:
        - source/target SQL bindingleri task basinda bir kez evaluate edilir.
        - source/target SQL sonucu tam olarak 1 satir 1 kolon olmalidir.
        - Cozumlenen where task_config["_resolved_where"] icine yazilir.
        """
        bindings = task_config.get("bindings") or []
        if not isinstance(bindings, list) or not bindings:
            return task_config

        result = dict(task_config)
        where_clause = str(result.get("_resolved_where") or result.get("where") or "").strip()
        if not where_clause:
            return result

        ctx = context or {}
        resolved_values: dict[str, Any] = {}

        for item in bindings:
            if not isinstance(item, dict):
                continue
            name = str(item.get("variable_name") or "").strip()
            source = str(item.get("binding_source") or "").strip()
            if not name:
                continue

            if source == "default":
                resolved_values[name] = item.get("default_value")
            elif source == "airflow_variable":
                key = str(item.get("airflow_variable_key") or "").strip()
                if not key:
                    raise ConfigError(f"Binding '{name}' icin airflow_variable_key zorunludur.")
                if key not in ctx:
                    raise ConfigError(f"Airflow Variable '{key}' bulunamadi.")
                resolved_values[name] = ctx[key]
            elif source in {"source", "target"}:
                sql = str(item.get("sql") or "").strip()
                if not sql:
                    raise ConfigError(f"Binding '{name}' icin sql zorunludur.")
                session = source_session if source == "source" else target_session
                resolved_values[name] = self._run_scalar_sql(session, sql, name=name, binding_source=source)
            else:
                raise ConfigError(f"Binding '{name}' icin gecersiz binding_source: {source!r}")

        def _replace(match: re.Match) -> str:
            param_name = match.group(1)
            if param_name not in resolved_values:
                raise ConfigError(f"Where Clause parametresi icin binding bulunamadi: :{param_name}")
            return self._to_sql_literal(resolved_values[param_name])

        result["_resolved_where"] = _PARAM_RE.sub(_replace, where_clause)
        return result

    def _resolve_legacy_string(self, value: str, task_config: dict, context: dict) -> str:
        def _replace(match: re.Match) -> str:
            binding_source = match.group(1)
            key = match.group(2)

            if binding_source == "literal":
                return key

            if binding_source == "airflow_var":
                if key not in context:
                    raise ConfigError(f"Airflow Variable '{key}' context'te bulunamadi.")
                return str(context[key])

            if binding_source in ("source", "target"):
                cfg_key = f"_{binding_source}_{key}"
                if cfg_key not in task_config:
                    _log.warning(
                        "Binding '%s.%s' task_config'te bulunamadi (%s); ifade oldugu gibi birakildi.",
                        binding_source,
                        key,
                        cfg_key,
                    )
                    return match.group(0)
                return str(task_config[cfg_key])

            return match.group(0)

        return _LEGACY_BINDING_RE.sub(_replace, value)

    def _run_scalar_sql(self, session: Any, sql: str, *, name: str, binding_source: str) -> Any:
        cursor = None
        try:
            cursor = session.cursor()
            cursor.execute(sql)
            first = cursor.fetchone()
            if first is None:
                raise ConfigError(
                    f"Binding '{name}' ({binding_source}) SQL sonucu 1x1 olmali; 0 satir dondu."
                )

            if isinstance(first, (tuple, list)):
                if len(first) != 1:
                    raise ConfigError(
                        f"Binding '{name}' ({binding_source}) SQL sonucu 1x1 olmali; {len(first)} kolon dondu."
                    )
                value = first[0]
            else:
                value = first

            second = cursor.fetchone()
            if second is not None:
                raise ConfigError(
                    f"Binding '{name}' ({binding_source}) SQL sonucu 1x1 olmali; birden fazla satir dondu."
                )
            return value
        except ConfigError:
            raise
        except Exception as exc:
            raise ConfigError(
                f"Binding '{name}' ({binding_source}) SQL calistirilamadi: {exc}"
            ) from exc
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass

    def _to_sql_literal(self, value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, int):
            return str(value)
        if isinstance(value, float):
            if not math.isfinite(value):
                raise ConfigError("Binding degeri NaN/Inf olamaz.")
            return str(value)
        if isinstance(value, (datetime, date)):
            text = value.isoformat(sep=" ") if isinstance(value, datetime) else value.isoformat()
            return "'" + text.replace("'", "''") + "'"

        text = str(value)
        return "'" + text.replace("'", "''") + "'"
