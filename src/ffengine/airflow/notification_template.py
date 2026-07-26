"""F1.3b — Named, admin-editable mail templates for operational notifications.

A global set of named templates (subject + full HTML body with placeholders) is
stored in a single Airflow Variable. Each DAG's notification config may select a
template by name; the callback resolves the name at send time (unknown ⇒ Default).

Rendering is a **controlled placeholder substitution only** (``{{name}}`` → an
escaped value from a fixed map) — never Jinja/eval. Placeholder VALUES are
HTML-escaped in the body and the subject strips CR/LF (header-injection guard).
Only sanitized top-level error fields are ever exposed (raw ``details``/
``sql_preview`` are never placeholders — INV-5). The metadata-only footer is
always appended by the system.
"""

from __future__ import annotations

import html as _html
import logging
import re
from typing import Any

_log = logging.getLogger(__name__)

TEMPLATES_VARIABLE_KEY = "ffengine_mail_templates"
DEFAULT_TEMPLATE_NAME = "Default"

_NAME_RE = re.compile(r"^[A-Za-z0-9 _-]{1,60}$")
_PLACEHOLDER_RE = re.compile(r"\{\{(\w+)\}\}")
_NAME_MAX = 60

_FOOTER_HTML = (
    "<p style='color:#666;font-size:12px'>This message contains run metadata "
    "only; no data rows are included.</p>"
)

DEFAULT_TEMPLATE: dict[str, str] = {
    "subject": "{{status}} [FFEngine] DAG {{dag_id}}",
    "html_body": (
        "<h3>{{status}} · FFEngine · DAG {{dag_id}}</h3>"
        "<table cellpadding='4' cellspacing='0'>"
        "<tr><td style='font-weight:600'>DAG</td><td>{{dag_id}}</td></tr>"
        "<tr><td style='font-weight:600'>Run</td><td>{{run_id}}</td></tr>"
        "<tr><td style='font-weight:600'>State</td><td>{{status}}</td></tr>"
        "<tr><td style='font-weight:600'>Logical date</td><td>{{logical_date}}</td></tr>"
        "<tr><td style='font-weight:600'>Started</td><td>{{started}}</td></tr>"
        "<tr><td style='font-weight:600'>Ended</td><td>{{ended}}</td></tr>"
        "<tr><td style='font-weight:600'>Rows transferred</td><td>{{rows}}</td></tr>"
        "{{error_block}}"
        "<tr><td style='font-weight:600'>Log</td>"
        "<td><a href='{{log_url}}'>{{log_url}}</a></td></tr>"
        "</table>"
    ),
}

PLACEHOLDERS: list[dict[str, str]] = [
    {"name": "status", "description": "FAILED or SUCCEEDED"},
    {"name": "dag_id", "description": "DAG id"},
    {"name": "run_id", "description": "DAG run id"},
    {"name": "state", "description": "failed / success"},
    {"name": "started", "description": "Run start timestamp"},
    {"name": "ended", "description": "Run end timestamp"},
    {"name": "logical_date", "description": "Logical (schedule) date"},
    {"name": "rows", "description": "Rows transferred (best-effort)"},
    {"name": "log_url", "description": "Airflow task log URL"},
    {"name": "error_type", "description": "Error class (failure only)"},
    {"name": "error_code", "description": "Error code (failure only)"},
    {"name": "error_message", "description": "Sanitized error message (failure only)"},
    {"name": "error_block", "description": "Standard error rows; empty on success"},
]


# --------------------------------------------------------------------------
# Storage (Airflow Variable)
# --------------------------------------------------------------------------

def _valid_shape(name: Any, tpl: Any) -> bool:
    return (
        bool(str(name or "").strip())
        and isinstance(tpl, dict)
        and isinstance(tpl.get("subject"), str)
        and isinstance(tpl.get("html_body"), str)
    )


def _stored_templates() -> dict[str, dict[str, str]]:
    """Raw user-stored templates from the Variable (built-in Default NOT merged)."""
    try:
        from airflow.models import Variable

        raw = Variable.get(
            TEMPLATES_VARIABLE_KEY, default_var=None, deserialize_json=True
        )
    except Exception:  # noqa: BLE001 - missing/malformed ⇒ empty, fall back to default
        raw = None
    out: dict[str, dict[str, str]] = {}
    if isinstance(raw, dict):
        for name, tpl in raw.items():
            if _valid_shape(name, tpl):
                out[str(name)] = {
                    "subject": str(tpl["subject"]),
                    "html_body": str(tpl["html_body"]),
                }
    return out


def load_templates() -> dict[str, dict[str, str]]:
    """All templates, with the built-in Default merged in (user override wins)."""
    templates = _stored_templates()
    templates.setdefault(DEFAULT_TEMPLATE_NAME, dict(DEFAULT_TEMPLATE))
    return templates


def load_template(name: Any) -> dict[str, str]:
    """Resolve a template by name; unknown/empty ⇒ Default."""
    templates = load_templates()
    key = str(name or "").strip() or DEFAULT_TEMPLATE_NAME
    return templates.get(key) or templates[DEFAULT_TEMPLATE_NAME]


def _validate_name(name: Any) -> str:
    value = str(name or "").strip()
    if not _NAME_RE.match(value):
        raise ValueError(
            f"Template name must be 1-{_NAME_MAX} chars of letters, digits, "
            "space, '_' or '-'."
        )
    return value


def save_template(name: Any, subject: Any, html_body: Any) -> dict[str, str]:
    """Upsert one named template (fail-loud validation)."""
    from airflow.models import Variable

    clean_name = _validate_name(name)
    clean_subject = _strip_header(str(subject or "").strip())
    clean_body = str(html_body or "").strip()
    if not clean_subject:
        raise ValueError("Template subject cannot be empty.")
    if not clean_body:
        raise ValueError("Template html_body cannot be empty.")
    templates = _stored_templates()
    templates[clean_name] = {"subject": clean_subject, "html_body": clean_body}
    Variable.set(TEMPLATES_VARIABLE_KEY, templates, serialize_json=True)
    return {"subject": clean_subject, "html_body": clean_body}


def delete_template(name: Any) -> None:
    """Delete a named template. Deleting Default just drops any override."""
    from airflow.models import Variable

    clean_name = str(name or "").strip()
    templates = _stored_templates()
    if clean_name in templates:
        del templates[clean_name]
        Variable.set(TEMPLATES_VARIABLE_KEY, templates, serialize_json=True)


# --------------------------------------------------------------------------
# Rendering (controlled substitution; never raises)
# --------------------------------------------------------------------------

def _strip_header(text: str) -> str:
    return text.replace("\r", " ").replace("\n", " ").strip()


def _value_map(meta: Any, label: str) -> dict[str, str]:
    meta = meta if isinstance(meta, dict) else {}
    error = meta.get("error") if isinstance(meta.get("error"), dict) else {}
    rows = meta.get("rows")
    return {
        "status": label,
        "dag_id": str(meta.get("dag_id") or "-"),
        "run_id": str(meta.get("run_id") or "-"),
        "state": str(meta.get("state") or "-"),
        "started": str(meta.get("started") or "-"),
        "ended": str(meta.get("ended") or "-"),
        "logical_date": str(meta.get("logical_date") or "-"),
        "rows": "-" if rows is None else str(rows),
        "log_url": str(meta.get("log_url") or "-"),
        "error_type": str(error.get("error_type") or ""),
        "error_code": str(error.get("error_code") or ""),
        "error_message": str(error.get("message") or "")[:500],
    }


def _substitute(text: str, values: dict[str, str], escape: bool) -> str:
    def repl(match: re.Match) -> str:
        key = match.group(1)
        if key not in values:
            return match.group(0)  # unknown placeholder ⇒ literal
        value = values[key]
        return _html.escape(value, quote=True) if escape else value

    return _PLACEHOLDER_RE.sub(repl, text)


def _html_row(name: str, value: str) -> str:
    return (
        f"<tr><td style='font-weight:600'>{_html.escape(name)}</td>"
        f"<td>{_html.escape(value)}</td></tr>"
    )


def _error_block_html(error: Any) -> str:
    if not isinstance(error, dict):
        return ""
    rows = [
        ("Error type", str(error.get("error_type") or "")),
        ("Error code", str(error.get("error_code") or "")),
        ("Error message", str(error.get("message") or "")[:500]),
    ]
    return "".join(_html_row(name, value) for name, value in rows if value)


def _render(template: dict[str, str], meta: Any, label: str) -> tuple[str, str]:
    values = _value_map(meta, label)
    subject = _strip_header(_substitute(str(template.get("subject") or ""), values, False))
    body = _substitute(str(template.get("html_body") or ""), values, True)
    # error_block is system-built HTML (values escaped inside) ⇒ inserted raw, last.
    body = body.replace("{{error_block}}", _error_block_html((meta or {}).get("error")))
    return subject, body + _FOOTER_HTML


def render_template(template: Any, meta: Any, label: str) -> tuple[str, str]:
    """Render (subject, html). Never raises — falls back to the built-in Default."""
    tpl = template if isinstance(template, dict) else DEFAULT_TEMPLATE
    try:
        return _render(tpl, meta, label)
    except Exception:  # noqa: BLE001
        try:
            return _render(DEFAULT_TEMPLATE, meta, label)
        except Exception:  # noqa: BLE001
            dag_id = str((meta or {}).get("dag_id") or "-")
            return (
                f"{label} [FFEngine] DAG {dag_id}",
                f"<p>{label} — DAG {_html.escape(dag_id)}</p>{_FOOTER_HTML}",
            )


def sample_meta(kind: str) -> dict[str, Any]:
    """Fixed sample metadata for the admin Preview."""
    error = None
    if kind == "failure":
        error = {
            "error_type": "ConnectionError",
            "error_code": "E_TARGET_WRITE",
            "message": "batch write to target failed",
            "details": {"db_error_message": "hidden", "sql_preview": "hidden"},
        }
    return {
        "dag_id": "orders_dag",
        "run_id": "manual__2026-01-01T00:00:00+00:00",
        "state": "failed" if kind == "failure" else "success",
        "started": "2026-01-01T00:00:00+00:00",
        "ended": "2026-01-01T00:05:00+00:00",
        "logical_date": "2026-01-01T00:00:00+00:00",
        "rows": 1234,
        "log_url": "http://airflow.local/log?dag=orders_dag",
        "error": error,
    }
