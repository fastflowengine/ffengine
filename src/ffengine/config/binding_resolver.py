"""
C05 - Binding cozumleme.

BindingResolver destekleri:
- Legacy template syntax: {{ source.X }}, {{ target.X }}, {{ literal.X }}, {{ airflow_var.KEY }}
- Task-local binding syntax: {{ param }} + bindings[] listesi
- DAG Param syntax: {{ dag.param }}
- Airflow Variable syntax: {{ airflow.REAL_KEY }}
"""

from __future__ import annotations

import logging
import math
import re
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from ffengine.errors.exceptions import ConfigError

_log = logging.getLogger(__name__)

# Legacy: {{ source.col }}, {{ literal.val }}, {{ airflow_var.KEY }}
_LEGACY_BINDING_RE = re.compile(
    r"\{\{\s*(source|target|literal|airflow_var)\.(\S+?)\s*\}\}"
)
_SIMPLE_PARAM_RE = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")
_DAG_PARAM_RE = re.compile(
    r"\{\{\s*dag\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\}"
)
_AIRFLOW_PARAM_RE = re.compile(r"\{\{\s*airflow\.([^\s{}]+)\s*\}\}")
_OBSOLETE_PARAM_RE = re.compile(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)")
_DATETIME_TIMESPEC = "microseconds"


class BindingResolver:
    """Task baglama/parametre cozumleyici."""

    def resolve(self, task_config: dict, context: dict | None = None) -> dict:
        """Legacy template bindinglerini cozer ve _resolved_where yazar."""
        ctx = context or {}
        result = dict(task_config)

        where = task_config.get("where")
        if where:
            result["_resolved_where"] = self._resolve_legacy_string(
                str(where), task_config, ctx
            )

        return result

    def resolve_sql_bindings(
        self,
        task_config: dict,
        *,
        context: dict | None,
        source_session: Any,
        target_session: Any,
        where_dialect: Any = None,
    ) -> dict:
        """
        Yeni UI binding modelini cozer.

        Notlar:
        - source/target SQL bindingleri task basinda bir kez evaluate edilir.
        - source/target SQL sonucu tam olarak 1 satir 1 kolon olmalidir.
        - Cozumlenen where task_config["_resolved_where"] icine yazilir.
        - `inline_sql` de ayni sablonlari destekler (2026-08-27): kullanici
          `source_type='sql'` ile SQL'i kendi yazdiginda parametreyi WHERE
          alanina degil sorgunun ICINE koyar. Onceden yalnizca `where`
          cozuldugu icin `{{ dag.x }}` veritabanina ham gidiyor ve
          "syntax error at or near {" hatasi veriyordu.
        """
        result = dict(task_config)
        where_clause = str(
            result.get("_resolved_where") or result.get("where") or ""
        ).strip()
        inline_sql = str(result.get("inline_sql") or "").strip()
        if not where_clause and not inline_sql:
            return result

        obsolete = _OBSOLETE_PARAM_RE.search(where_clause or inline_sql)
        if obsolete:
            name = obsolete.group(1)
            raise ConfigError(
                f"Obsolete parameter syntax; replace :{name} with {{{{ {name} }}}}."
            )

        bindings = task_config.get("bindings") or []
        if not isinstance(bindings, list):
            raise ConfigError("bindings must be a list.")
        has_parameter = any(
            pattern.search(where_clause) or pattern.search(inline_sql)
            for pattern in (_SIMPLE_PARAM_RE, _DAG_PARAM_RE, _AIRFLOW_PARAM_RE)
        )
        if not bindings and not has_parameter:
            return result

        ctx = context or {}
        dag_binding_values = self._namespace(ctx, "binding_values")
        dag_run_conf = self._namespace(ctx, "dag_run_conf")
        dag_airflow_params = self._namespace(ctx, "airflow_params")
        dag_binding_producers = self._namespace(ctx, "binding_producers")
        airflow_vars = self._namespace(ctx, "airflow_variables", fallback=ctx)
        local_values = self._resolve_local_binding_values(
            bindings,
            airflow_vars=airflow_vars,
            source_session=source_session,
            target_session=target_session,
        )

        def _replace_local(match: re.Match) -> str:
            param_name = match.group(1)
            if param_name in local_values:
                value = local_values[param_name]
            else:
                value = self._resolve_dag_value(
                    param_name,
                    dag_run_conf=dag_run_conf,
                    binding_values=dag_binding_values,
                    airflow_params=dag_airflow_params,
                    binding_producers=dag_binding_producers,
                    legacy=True,
                )
            return self._to_sql_literal(value, where_dialect=where_dialect)

        def _replace_dag(match: re.Match) -> str:
            param_name = match.group(1)
            value = self._resolve_dag_value(
                param_name,
                dag_run_conf=dag_run_conf,
                binding_values=dag_binding_values,
                airflow_params=dag_airflow_params,
                binding_producers=dag_binding_producers,
                legacy=False,
            )
            return self._to_sql_literal(value, where_dialect=where_dialect)

        def _replace_airflow(match: re.Match) -> str:
            key = match.group(1)
            if key not in airflow_vars:
                raise ConfigError(
                    f"Airflow Variable '{key}' bulunamadi for "
                    f"{{{{ airflow.{key} }}}}."
                )
            return self._to_sql_literal(airflow_vars[key], where_dialect=where_dialect)

        def _substitute(text: str) -> str:
            out = _DAG_PARAM_RE.sub(_replace_dag, text)
            out = _AIRFLOW_PARAM_RE.sub(_replace_airflow, out)
            return _SIMPLE_PARAM_RE.sub(_replace_local, out)

        if where_clause:
            result["_resolved_where"] = _substitute(where_clause)
        # `inline_sql` YERINDE guncellenir: SourceReader onu dogrudan okur
        # (ayri bir `_resolved_*` anahtari beklemez), bu yuzden ayni alan
        # cozulmus haliyle degistirilir.
        if inline_sql:
            result["inline_sql"] = _substitute(inline_sql)
        return result

    def _resolve_local_binding_values(
        self,
        bindings: list[dict[str, Any]],
        *,
        airflow_vars: dict[str, Any],
        source_session: Any,
        target_session: Any,
    ) -> dict[str, Any]:
        resolved: dict[str, Any] = {}
        for item in bindings:
            name = str(item.get("variable_name") or "").strip()
            if not name:
                continue
            resolved[name] = self._resolve_binding_item(
                item, airflow_vars, source_session, target_session
            )
        return resolved

    def _resolve_dag_value(
        self,
        name: str,
        *,
        dag_run_conf: dict[str, Any],
        binding_values: dict[str, Any],
        airflow_params: dict[str, Any] | None = None,
        binding_producers: dict[str, str] | None = None,
        legacy: bool,
    ) -> Any:
        """DAG parametresinin degerini uc kaynaktan cozer.

        Oncelik SIRASI anlamlidir ve daralandan genise gider:

        1. ``binding_values`` -- Binding task'inin uretttigi deger (en ozel:
           bu kosu icin hesaplanmistir)
        2. ``dag_run_conf`` -- tetiklemede verilen conf (kullanici bu kosu
           icin acikca gecmistir)
        3. ``airflow_params`` -- DAG parametresinin varsayilani (en genel)

        ``airflow_params`` 2026-08-27'de eklendi: ``dag_params`` ile
        tanimlanan bir parametre Airflow ``Param``ina, oradan
        ``context["params"]``e ve ``build_runtime_binding_context`` ile
        ``airflow_params``a dusuyordu -- fakat burada okunmadigi icin
        "no declared/runtime value" hatasi veriyordu. Operator tarafi
        (``_build_external_run_contract``) ucunu de zaten tariyor; bu
        fonksiyon o sozlesmeyle hizalandi.
        """
        if name in binding_values:
            return binding_values[name]
        if name in dag_run_conf:
            return dag_run_conf[name]
        # FAIL-LOUD (INV-6): bir Binding task'i bu kosu icin bu parametreyi
        # URETIYORSA, varsayilana dusmek sessizce BAYAT deger kullanmaktir --
        # akis yesil kalir, sonuc yanlis olur. Bu durum pratikte tarama
        # listesi eksikliginden dogar (`_reference_expression` bir alani
        # atlar -> compiled binding kaynagi bos kalir). Varsayilana dusus
        # yalnizca gercekten uretici olmadiginda mesrudur.
        if binding_producers and name in binding_producers:
            raise ConfigError(
                f"DAG parameter '{name}' is produced by Binding task "
                f"'{binding_producers[name]}' but no value reached this task; "
                "refusing to fall back to the parameter default (stale value). "
                "This usually means the expression field carrying the "
                "template is not scanned by dag_param_flow."
                "_reference_expression."
            )
        if airflow_params and name in airflow_params:
            return airflow_params[name]
        syntax = f"{{{{ {name} }}}}" if legacy else f"{{{{ dag.{name} }}}}"
        raise ConfigError(
            "Where Clause parameter has no declared/runtime value: " + syntax
        )

    def resolve_binding_values(
        self,
        bindings: list[dict[str, Any]],
        *,
        context: dict | None,
        source_session: Any,
        target_session: Any,
    ) -> dict[str, Any]:
        ctx = context or {}
        airflow_vars = self._namespace(ctx, "airflow_variables", fallback=ctx)
        resolved: dict[str, Any] = {}
        for item in bindings:
            name = str(item.get("variable_name") or "").strip()
            if not name:
                continue
            resolved[name] = self._resolve_binding_item(
                item, airflow_vars, source_session, target_session
            )
        return resolved

    def _resolve_binding_item(
        self,
        item: dict[str, Any],
        airflow_vars: dict[str, Any],
        source_session: Any,
        target_session: Any,
    ) -> Any:
        name = str(item.get("variable_name") or "").strip()
        source = str(item.get("binding_source") or "").strip()
        if source == "default":
            return item.get("default_value")
        if source == "airflow_variable":
            key = str(item.get("airflow_variable_key") or "").strip()
            if not key or key not in airflow_vars:
                raise ConfigError(f"Airflow Variable '{key}' bulunamadi.")
            return airflow_vars[key]
        if source in {"source", "target"}:
            sql = str(item.get("sql") or "").strip()
            if not sql:
                raise ConfigError(f"Binding '{name}' icin sql zorunludur.")
            session = source_session if source == "source" else target_session
            return self._run_scalar_sql(
                session, sql, name=name, binding_source=source
            )
        raise ConfigError(
            f"Binding '{name}' icin gecersiz binding_source: {source!r}"
        )

    @staticmethod
    def _namespace(
        context: dict[str, Any],
        key: str,
        *,
        fallback: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        value = context.get(key)
        if isinstance(value, dict):
            return value
        return fallback or {}

    def _resolve_legacy_string(
        self, value: str, task_config: dict, context: dict
    ) -> str:
        airflow_vars = self._namespace(context, "airflow_variables", fallback=context)
        return _LEGACY_BINDING_RE.sub(
            lambda match: self._resolve_legacy_match(
                match, task_config, airflow_vars
            ),
            value,
        )

    def _resolve_legacy_match(
        self,
        match: re.Match,
        task_config: dict,
        context: dict,
    ) -> str:
        binding_source = match.group(1)
        key = match.group(2)
        if binding_source == "literal":
            return key
        if binding_source == "airflow_var":
            if key not in context:
                raise ConfigError(f"Airflow Variable '{key}' context'te bulunamadi.")
            return str(context[key])
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

    def _run_scalar_sql(
        self, session: Any, sql: str, *, name: str, binding_source: str
    ) -> Any:
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

    def _to_sql_literal(self, value: Any, *, where_dialect: Any = None) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            if self._is_postgres_dialect(where_dialect):
                return "TRUE" if value else "FALSE"
            return "1" if value else "0"
        if isinstance(value, int):
            return str(value)
        if isinstance(value, Decimal):
            if not value.is_finite():
                raise ConfigError("Binding degeri NaN/Inf olamaz.")
            return str(value)
        if isinstance(value, float):
            if not math.isfinite(value):
                raise ConfigError("Binding degeri NaN/Inf olamaz.")
            return str(value)
        if isinstance(value, datetime):
            return self._datetime_to_sql_literal(value, where_dialect=where_dialect)
        if isinstance(value, date):
            return self._date_to_sql_literal(value, where_dialect=where_dialect)

        return self._quoted_text(value, where_dialect=where_dialect)

    @classmethod
    def _quoted_text(cls, value: Any, *, where_dialect: Any = None) -> str:
        prefix = "N" if cls._is_mssql_dialect(where_dialect) else ""
        text = str(value).replace("'", "''")
        return f"{prefix}'{text}'"

    @classmethod
    def _date_to_sql_literal(cls, value: date, *, where_dialect: Any = None) -> str:
        text = value.isoformat().replace("'", "''")
        if cls._is_mssql_dialect(where_dialect):
            return f"CAST(N'{text}' AS date)"
        return f"DATE '{text}'"

    @staticmethod
    def _dialect_name(dialect: Any) -> str:
        if dialect is None:
            return ""
        if isinstance(dialect, str):
            return dialect.strip().lower()
        return type(dialect).__name__.strip().lower()

    @classmethod
    def _is_postgres_dialect(cls, dialect: Any) -> bool:
        name = cls._dialect_name(dialect)
        return name in {"postgresdialect", "postgresqldialect"} or "postgres" in name

    @classmethod
    def _is_mssql_dialect(cls, dialect: Any) -> bool:
        name = cls._dialect_name(dialect)
        return name in {"mssqldialect", "sqlserverdialect"} or "mssql" in name

    @classmethod
    def _datetime_to_sql_literal(
        cls, value: datetime, *, where_dialect: Any = None
    ) -> str:
        dt = value
        if dt.tzinfo is not None:
            dt_utc = dt.astimezone(timezone.utc)
            if cls._is_postgres_dialect(where_dialect):
                text_tz = dt_utc.isoformat(
                    sep=" ", timespec=_DATETIME_TIMESPEC
                ).replace("'", "''")
                return f"TIMESTAMPTZ '{text_tz}'"
            dt = dt_utc.replace(tzinfo=None)
        text = dt.isoformat(sep=" ", timespec=_DATETIME_TIMESPEC).replace("'", "''")
        if cls._is_mssql_dialect(where_dialect):
            return f"CAST(N'{text}' AS datetime2(6))"
        return f"TIMESTAMP '{text}'"
