"""F1.3 — Operational notification (Community).

Thin, transparent wrapper over Airflow-native DAG-level callbacks that emails a
run's **metadata only** (dag/run/state/duration/rows/error summary/log link) —
never data rows. SMTP credentials live only in the referenced Airflow Connection
(``notify_conn_id``); nothing secret enters config, the DAG file, or logs.

Positioning: observability/alerting, not error handling. A notification/SMTP
failure is logged and swallowed so it can never drop the transfer (T-F1.3-1).
"""

from __future__ import annotations

import logging
from typing import Any, Callable

from ffengine.airflow.notification_template import load_template, render_template

_log = logging.getLogger(__name__)


def build_notification_callbacks(notifications: Any) -> dict[str, Any]:
    """Return the DAG-level callback kwargs for a flow's notification policy.

    ``{}`` when notifications are absent/disabled (so the DAG is built exactly
    as before — backward compatible).
    """
    if not isinstance(notifications, dict):
        return {}
    triggers = notifications.get("notify_on") or []
    emails = [
        str(item).strip()
        for item in (notifications.get("notify_emails") or [])
        if str(item).strip()
    ]
    conn_id = str(notifications.get("notify_conn_id") or "").strip()
    if not triggers or not emails or not conn_id:
        return {}
    template_name = str(notifications.get("notify_template") or "").strip()

    callbacks: dict[str, Any] = {}
    if "failure" in triggers:
        callbacks["on_failure_callback"] = _make_callback(
            "failure", emails, conn_id, template_name
        )
    if "success" in triggers:
        callbacks["on_success_callback"] = _make_callback(
            "success", emails, conn_id, template_name
        )
    return callbacks


def _make_callback(
    kind: str, emails: list[str], conn_id: str, template_name: str
) -> Callable[[Any], None]:
    def _callback(context: Any) -> None:
        _send_notification(kind, emails, conn_id, template_name, context)

    return _callback


def _send_notification(
    kind: str, emails: list[str], conn_id: str, template_name: str, context: Any
) -> None:
    """Build the message and send it via the native SmtpNotifier.

    Any failure (import, build, or SMTP send) is logged and swallowed — the
    notification must never fail the task/transfer (T-F1.3-1/INV-6).
    """
    try:
        subject, html_content = _build_message(kind, context, template_name)
        from airflow.providers.smtp.notifications.smtp import SmtpNotifier

        SmtpNotifier(
            to=list(emails),
            smtp_conn_id=conn_id,
            subject=subject,
            html_content=html_content,
        ).notify(context or {})
    except Exception as exc:  # noqa: BLE001 - alerting must never drop the transfer
        # Log only the exception *type* + conn_id — never the message, which
        # could carry SMTP server/credential detail (INV-5).
        _log.warning(
            "ffengine.notification.send_failed kind=%s conn_id=%s error_type=%s",
            kind,
            conn_id,
            type(exc).__name__,
        )


def _build_message(
    kind: str, context: Any, template_name: str = ""
) -> tuple[str, str]:
    """Return ``(subject, html)`` from run metadata via the selected template."""
    meta = _collect_metadata(kind, context)
    label = "FAILED" if kind == "failure" else "SUCCEEDED"
    template = load_template(template_name)
    return render_template(template, meta, label)


def _collect_metadata(kind: str, context: Any) -> dict[str, Any]:
    ctx = context if isinstance(context, dict) else {}
    dag_run = ctx.get("dag_run")
    ti = ctx.get("ti") or ctx.get("task_instance")
    task_instances = _iter_task_instances(dag_run)
    return {
        "dag_id": _safe_str(_first_attr(dag_run, ti, "dag_id") or ctx.get("dag")),
        "run_id": _safe_str(
            _first_attr(dag_run, ti, "run_id") or ctx.get("run_id")
        ),
        "state": "failed" if kind == "failure" else "success",
        "started": _safe_str(getattr(dag_run, "start_date", None)),
        "ended": _safe_str(getattr(dag_run, "end_date", None)),
        "logical_date": _safe_str(
            getattr(dag_run, "logical_date", None) or ctx.get("logical_date")
        ),
        "rows": _pull_rows(task_instances or ([ti] if ti else [])),
        "log_url": _safe_str(getattr(ti, "log_url", "")),
        "error": (
            _pull_error_summary(task_instances or ([ti] if ti else []))
            if kind == "failure"
            else None
        ),
    }


def _iter_task_instances(dag_run: Any) -> list[Any]:
    try:
        tis = dag_run.get_task_instances()
        return list(tis or [])
    except Exception:  # noqa: BLE001 - best-effort; absence just omits detail
        return []


def _ti_xcom(ti: Any, key: str) -> Any:
    try:
        return ti.xcom_pull(task_ids=getattr(ti, "task_id", None), key=key)
    except Exception:  # noqa: BLE001
        return None


def _pull_rows(task_instances: list[Any]) -> int | None:
    total = 0
    found = False
    for ti in task_instances:
        if ti is None:
            continue
        value = _ti_xcom(ti, "rows_transferred")
        if isinstance(value, (int, float)):
            total += int(value)
            found = True
    return total if found else None


def _pull_error_summary(task_instances: list[Any]) -> dict[str, Any] | None:
    for ti in task_instances:
        if ti is None:
            continue
        value = _ti_xcom(ti, "error_summary")
        if isinstance(value, dict):
            return value
    return None


def _first_attr(primary: Any, secondary: Any, name: str) -> Any:
    return getattr(primary, name, None) or getattr(secondary, name, None)


def _safe_str(value: Any) -> str:
    if value is None:
        return "-"
    text = str(value).strip()
    return text or "-"
