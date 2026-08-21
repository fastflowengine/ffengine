from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from ffengine.config.dbt_contract import dbt_vars_expression_text


TRIGGER_SOURCE = "__dag_run_conf__"
AMBIGUOUS_SOURCE = "__ambiguous__"
BUILTIN_DAG_PARAM_BINDING_ERROR = (
    "Built-in DAG parameter 'log_level' cannot be assigned by a Binding task."
)

_SIMPLE_PARAM_RE = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")
_DAG_PARAM_RE = re.compile(r"\{\{\s*dag\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")


def validate_binding_target_is_custom(name: str) -> None:
    if name == "log_level":
        raise ValueError(BUILTIN_DAG_PARAM_BINDING_ERROR)


@dataclass(frozen=True)
class DagParameterFlowPlan:
    input_sources: dict[str, dict[str, str]]
    references: dict[str, set[str]]
    assignments: dict[str, set[str]]

    def binding_sources_for(self, task_id: str) -> dict[str, str]:
        sources = self.input_sources.get(task_id, {})
        return {
            name: sources[name]
            for name in sorted(self.references.get(task_id, set()))
            if sources.get(name) not in {None, TRIGGER_SOURCE, AMBIGUOUS_SOURCE}
        }

    def binding_task_ids_for(self, task_id: str) -> list[str]:
        return sorted(set(self.binding_sources_for(task_id).values()))

    def superseded_sources_for(self, task_id: str) -> dict[str, str]:
        sources = self.input_sources.get(task_id, {})
        return {
            name: sources[name]
            for name in self.assignments.get(task_id, set())
            if name in sources
        }


def _declared_names(dag_params: list[dict[str, Any]]) -> set[str]:
    names = {str(item.get("name") or "").strip() for item in dag_params}
    names.discard("")
    return names


def _task_ids(task_defs: list[dict[str, Any]]) -> list[str]:
    ids = [str(item.get("task_group_id") or "").strip() for item in task_defs]
    if any(not task_id for task_id in ids):
        raise ValueError("task_group_id is required for each flow_task.")
    if len(ids) != len(set(ids)):
        raise ValueError("task_group_id must be unique.")
    return ids


def _parents_by_task(
    task_defs: list[dict[str, Any]], task_ids: list[str]
) -> dict[str, list[str]]:
    known = set(task_ids)
    parents: dict[str, list[str]] = {}
    for task_id, task in zip(task_ids, task_defs):
        raw = task.get("depends_on") or []
        if not isinstance(raw, list):
            raise ValueError(f"depends_on must be a list: task_group_id={task_id}")
        values = [str(item or "").strip() for item in raw if str(item or "").strip()]
        invalid = [item for item in values if item not in known or item == task_id]
        if invalid:
            raise ValueError(f"depends_on contains invalid task_group_id: {invalid[0]}")
        parents[task_id] = list(dict.fromkeys(values))
    return parents


def _topological_order(
    task_ids: list[str], parents: dict[str, list[str]]
) -> list[str]:
    remaining = {task_id: len(parents[task_id]) for task_id in task_ids}
    children = {task_id: [] for task_id in task_ids}
    for child, upstream_ids in parents.items():
        for upstream in upstream_ids:
            children[upstream].append(child)
    ready = [task_id for task_id in task_ids if remaining[task_id] == 0]
    ordered: list[str] = []
    while ready:
        task_id = ready.pop(0)
        ordered.append(task_id)
        for child in children[task_id]:
            remaining[child] -= 1
            if remaining[child] == 0:
                ready.append(child)
    if len(ordered) != len(task_ids):
        raise ValueError("depends_on cycle tespit edildi.")
    return ordered


def _binding_assignments(
    task: dict[str, Any], declared: set[str]
) -> set[str]:
    if str(task.get("task_type") or "") != "binding":
        return set()
    assigned: set[str] = set()
    for item in list(task.get("bindings") or []):
        source = str(item.get("binding_source") or "").strip()
        if source not in {"source", "target", "default"}:
            raise ValueError(
                "Binding tasks support source, target, or default binding sources."
            )
        name = str(item.get("variable_name") or "").strip()
        validate_binding_target_is_custom(name)
        if name not in declared:
            raise ValueError(
                f"Binding target is not a declared DAG parameter: {name}"
            )
        assigned.add(name)
    return assigned


def _reference_expression(task: dict[str, Any]) -> str:
    """Per-type text scanned for {{ p }} / {{ dag.p }} param references.

    Every task type that consumes DAG parameters MUST contribute its
    expression source here (mirrored in api_app and app.js); otherwise its
    compiled binding sources are silently empty (F3.2 / INV-6).
    """
    task_type = str(task.get("task_type") or "")
    if task_type == "script_run":
        return str(task.get("script_sql") or "")
    if task_type == "dbt":
        return dbt_vars_expression_text(task)
    # F1.5 — dosya yollari da `{{ p }}` sablonu tasir ve operator bunlari
    # render eder (`_render_file_paths`). Burada taranmazlarsa binding
    # kaynaklari sessizce bos kalir ve calisma aninda "cozulemeyen deger"
    # hatasina donusur -- yukaridaki sozlesmenin tam olarak uyardigi durum.
    return "\n".join(
        str(task.get(field) or "")
        for field in ("where", "file_path", "target_file_path")
    )


def _task_references(task: dict[str, Any], declared: set[str]) -> set[str]:
    if str(task.get("task_type") or "") == "binding":
        return set()
    expression = _reference_expression(task)
    local = {
        str(item.get("variable_name") or "").strip()
        for item in list(task.get("bindings") or [])
    }
    simple = set(_SIMPLE_PARAM_RE.findall(expression)) - local
    explicit = set(_DAG_PARAM_RE.findall(expression))
    undeclared = sorted(explicit - declared)
    if undeclared:
        raise ValueError(
            "DAG parameter reference is not declared: " + ", ".join(undeclared)
        )
    return explicit | (simple & declared)


def _incoming_sources(
    names: set[str], parents: list[str], outputs: dict[str, dict[str, str]]
) -> dict[str, str]:
    if not parents:
        return {name: TRIGGER_SOURCE for name in names}
    incoming: dict[str, str] = {}
    for name in names:
        sources = {outputs[parent][name] for parent in parents}
        incoming[name] = sources.pop() if len(sources) == 1 else AMBIGUOUS_SOURCE
    return incoming


def compile_dag_parameter_flow(
    dag_params: list[dict[str, Any]], task_defs: list[dict[str, Any]]
) -> DagParameterFlowPlan:
    declared = _declared_names(dag_params)
    task_ids = _task_ids(task_defs)
    tasks = dict(zip(task_ids, task_defs))
    parents = _parents_by_task(task_defs, task_ids)
    ordered = _topological_order(task_ids, parents)
    assignments = {
        task_id: _binding_assignments(tasks[task_id], declared)
        for task_id in task_ids
    }
    references = {
        task_id: _task_references(tasks[task_id], declared) for task_id in task_ids
    }
    inputs: dict[str, dict[str, str]] = {}
    outputs: dict[str, dict[str, str]] = {}
    for task_id in ordered:
        inputs[task_id] = _incoming_sources(declared, parents[task_id], outputs)
        ambiguous = sorted(
            name
            for name in references[task_id]
            if inputs[task_id].get(name) == AMBIGUOUS_SOURCE
        )
        if ambiguous:
            raise ValueError(
                f"Ambiguous DAG parameter source at task '{task_id}': "
                + ", ".join(ambiguous)
            )
        outputs[task_id] = dict(inputs[task_id])
        for name in assignments[task_id]:
            outputs[task_id][name] = task_id
    return DagParameterFlowPlan(inputs, references, assignments)
