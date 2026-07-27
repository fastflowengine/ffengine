"""F1.3 — notification callbacks: native wrapper, isolation, secret-free message."""

from __future__ import annotations

import logging
from unittest.mock import patch

from ffengine.airflow.notifications import (
    _build_message,
    build_notification_callbacks,
    deadline_notification_callback,
)

_SMTP_NOTIFIER = "airflow.providers.smtp.notifications.smtp.SmtpNotifier"


class _FakeTI:
    def __init__(self, xcom: dict, task_id: str = "run_orders"):
        self.task_id = task_id
        self.log_url = "http://airflow.local/log?dag=demo"
        self._xcom = xcom

    def xcom_pull(self, task_ids=None, key=None):
        return self._xcom.get(key)


class _FakeDagRun:
    def __init__(self, task_instances):
        self.dag_id = "demo_dag"
        self.run_id = "manual__2026-07-26T00:00:00"
        self.state = "failed"
        self.start_date = "2026-07-26T00:00:00+00:00"
        self.end_date = "2026-07-26T00:05:00+00:00"
        self.logical_date = "2026-07-26T00:00:00+00:00"
        self._tis = task_instances

    def get_task_instances(self):
        return self._tis


def _failure_context():
    error_summary = {
        "error_type": "ConnectionError",
        "error_code": "E_TARGET_WRITE",
        "message": "batch write to target failed",
        "details": {
            "db_error_message": "FATAL password authentication failed for user topsecret",
            "sql_preview": "INSERT INTO t VALUES (...)",
        },
    }
    ti = _FakeTI({"error_summary": error_summary, "rows_transferred": 42})
    dag_run = _FakeDagRun([ti])
    return {"dag_run": dag_run, "ti": ti}


# ---- build_notification_callbacks: correct callbacks per trigger set -------

def test_build_callbacks_per_notify_on():
    emails = ["a@x.com"]
    only_failure = build_notification_callbacks(
        {"notify_on": ["failure"], "notify_emails": emails, "notify_conn_id": "smtp"}
    )
    assert set(only_failure) == {"on_failure_callback"}

    only_success = build_notification_callbacks(
        {"notify_on": ["success"], "notify_emails": emails, "notify_conn_id": "smtp"}
    )
    assert set(only_success) == {"on_success_callback"}

    both = build_notification_callbacks(
        {
            "notify_on": ["failure", "success"],
            "notify_emails": emails,
            "notify_conn_id": "smtp",
        }
    )
    assert set(both) == {"on_failure_callback", "on_success_callback"}


def test_build_callbacks_disabled_returns_empty():
    assert build_notification_callbacks(None) == {}
    assert build_notification_callbacks({}) == {}
    assert (
        build_notification_callbacks(
            {"notify_on": ["failure"], "notify_emails": [], "notify_conn_id": "smtp"}
        )
        == {}
    )


# ---- T-F1.3-2: native wrapper (SmtpNotifier) fires --------------------------

def test_failure_callback_invokes_smtp_notifier():
    callbacks = build_notification_callbacks(
        {
            "notify_on": ["failure"],
            "notify_emails": ["ops@bank.example", "dev@bank.example"],
            "notify_conn_id": "smtp_default",
        }
    )
    with patch(_SMTP_NOTIFIER) as notifier_cls:
        callbacks["on_failure_callback"](_failure_context())

    notifier_cls.assert_called_once()
    kwargs = notifier_cls.call_args.kwargs
    assert kwargs["to"] == ["ops@bank.example", "dev@bank.example"]
    assert kwargs["smtp_conn_id"] == "smtp_default"
    assert "FAILED" in kwargs["subject"]
    # never a raw credential — only the connection id is passed
    assert "password" not in str(kwargs).lower()
    notifier_cls.return_value.notify.assert_called_once()


# ---- T-F1.3-1: SMTP error is swallowed, never drops the transfer -----------

def test_smtp_error_is_swallowed_and_logged(caplog):
    callbacks = build_notification_callbacks(
        {
            "notify_on": ["failure"],
            "notify_emails": ["a@x.com"],
            "notify_conn_id": "smtp_default",
        }
    )
    with patch(_SMTP_NOTIFIER) as notifier_cls:
        notifier_cls.return_value.notify.side_effect = RuntimeError("smtp down")
        with caplog.at_level(logging.WARNING):
            # must NOT raise
            result = callbacks["on_failure_callback"](_failure_context())

    assert result is None
    assert any(
        "notification.send_failed" in r.getMessage() for r in caplog.records
    )
    # the swallow log must not leak the exception message
    assert all("smtp down" not in r.getMessage() for r in caplog.records)


# ---- T-F1.3-3: credential-masked, metadata-only message --------------------

def test_message_is_secret_free_and_metadata_only():
    subject, html = _build_message("failure", _failure_context())

    assert subject.startswith("FAILED ")  # status-first
    assert "manual__" not in subject  # run suffix dropped from the subject
    assert "demo_dag" in subject
    assert "demo_dag" in html
    assert "manual__2026-07-26T00:00:00" in html  # run still in the body
    assert "42" in html  # row count metadata
    assert "batch write to target failed" in html  # actionable message
    assert "ConnectionError" in html

    # raw DB error details / sql_preview / credentials must NOT leak
    assert "topsecret" not in html
    assert "password authentication failed" not in html
    assert "INSERT INTO t" not in html
    # security expectation fixed in the body
    assert "no data rows" in html.lower()


def test_success_message_has_no_error_section():
    ti = _FakeTI({"rows_transferred": 7})
    dag_run = _FakeDagRun([ti])
    dag_run.state = "success"
    subject, html = _build_message("success", {"dag_run": dag_run, "ti": ti})
    assert subject.startswith("SUCCEEDED ")
    assert "7" in html
    assert "Error message" not in html


def test_callback_uses_named_template():
    custom = {"subject": "CUSTOM {{status}} {{dag_id}}", "html_body": "<p>{{status}}</p>"}
    callbacks = build_notification_callbacks(
        {
            "notify_on": ["failure"],
            "notify_emails": ["a@x.com"],
            "notify_conn_id": "smtp_default",
            "notify_template": "Banka-Kritik",
        }
    )
    with patch("ffengine.airflow.notifications.load_template", return_value=custom) as loader, \
            patch(_SMTP_NOTIFIER) as notifier_cls:
        callbacks["on_failure_callback"](_failure_context())

    loader.assert_called_once_with("Banka-Kritik")
    assert notifier_cls.call_args.kwargs["subject"].startswith("CUSTOM FAILED")


# ---- F1.3c deadline SyncCallback target -----------------------------------

def _deadline_context():
    return {
        "dag_run": {
            "dag_id": "orders_dag",
            "run_id": "manual__2026-07-26T00:00:00",
            "logical_date": "2026-07-26T00:00:00+00:00",
            "queued_at": "2026-07-26T00:00:00+00:00",
        },
        "deadline": {"id": "abc", "deadline_time": "2026-07-26T00:45:00+00:00"},
    }


def test_deadline_callback_sends_status_first_and_secret_free():
    with patch(_SMTP_NOTIFIER) as notifier_cls:
        result = deadline_notification_callback(
            context=_deadline_context(),
            emails=["ops@bank.example"],
            conn_id="smtp_default",
            template_name="",
        )
    assert result is None
    notifier_cls.assert_called_once()
    kwargs = notifier_cls.call_args.kwargs
    assert kwargs["to"] == ["ops@bank.example"]
    assert kwargs["smtp_conn_id"] == "smtp_default"
    assert kwargs["subject"].startswith("DEADLINE ")
    assert "orders_dag" in kwargs["subject"]
    assert "orders_dag" in kwargs["html_content"]
    assert "no data rows" in kwargs["html_content"].lower()
    assert "password" not in str(kwargs).lower()
    notifier_cls.return_value.notify.assert_called_once()


def test_deadline_callback_swallows_send_error(caplog):
    with patch(_SMTP_NOTIFIER) as notifier_cls:
        notifier_cls.return_value.notify.side_effect = RuntimeError("smtp down")
        with caplog.at_level(logging.WARNING):
            result = deadline_notification_callback(
                context=_deadline_context(),
                emails=["a@x.com"],
                conn_id="smtp_default",
            )
    assert result is None  # must never raise
    assert any("deadline_send_failed" in r.getMessage() for r in caplog.records)
    assert all("smtp down" not in r.getMessage() for r in caplog.records)
