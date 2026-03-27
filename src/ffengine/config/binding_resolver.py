"""
C05 — Binding çözümleme.

BindingResolver.resolve(task_config, context) WHERE ve string alanlardaki
{{ source.X }}, {{ target.X }}, {{ literal.X }}, {{ airflow_var.KEY }}
ifadelerini gerçek değerlerle değiştirir.

API_CONTRACTS.md:
  Çözümlenen WHERE → task_config["_resolved_where"]
"""

import logging
import re

from ffengine.errors.exceptions import ConfigError

_log = logging.getLogger(__name__)

# {{ source.col }}, {{ literal.val }}, {{ airflow_var.KEY }} — boşluğa toleranslı
_BINDING_RE = re.compile(
    r"\{\{\s*(source|target|literal|airflow_var)\.(\S+?)\s*\}\}"
)


class BindingResolver:
    """
    Desteklenen binding kaynakları (CONFIG_SCHEMA.md §Binding kaynakları):

    +---------------+-----------------------------------------------------+
    | Kaynak        | Davranış                                            |
    +---------------+-----------------------------------------------------+
    | source.X      | task_config["_source_X"] değerini okur              |
    | target.X      | task_config["_target_X"] değerini okur              |
    | literal.VAL   | VAL sabitini kullanır                               |
    | airflow_var.K | context["K"] değerini okur (Airflow Variable shim)  |
    +---------------+-----------------------------------------------------+

    context parametresi Airflow olmayan ortamlarda bir dict olarak geçilebilir
    (ör. test ortamı, CLI).
    """

    def resolve(self, task_config: dict, context: dict | None = None) -> dict:
        """
        Parameters
        ----------
        task_config : Normalize edilmiş task dict.
        context     : Airflow Variable değerleri veya test stub'ı.

        Returns
        -------
        task_config kopyası; WHERE varsa ``_resolved_where`` eklenir.
        """
        ctx = context or {}
        result = dict(task_config)

        where = task_config.get("where")
        if where:
            result["_resolved_where"] = self._resolve_string(
                where, task_config, ctx
            )

        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _resolve_string(
        self, value: str, task_config: dict, context: dict
    ) -> str:
        def _replace(match: re.Match) -> str:
            binding_source = match.group(1)
            key = match.group(2)

            if binding_source == "literal":
                return key

            if binding_source == "airflow_var":
                if key not in context:
                    raise ConfigError(
                        f"Airflow Variable '{key}' context'te bulunamadı."
                    )
                return str(context[key])

            if binding_source in ("source", "target"):
                cfg_key = f"_{binding_source}_{key}"
                if cfg_key not in task_config:
                    _log.warning(
                        "Binding '%s.%s' task_config'te bulunamadı (%s); "
                        "ifade olduğu gibi bırakıldı.",
                        binding_source,
                        key,
                        cfg_key,
                    )
                    return match.group(0)
                return str(task_config[cfg_key])

            return match.group(0)

        return _BINDING_RE.sub(_replace, value)
