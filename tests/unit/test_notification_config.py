"""F1.3 — notification config normalization (fail-loud, flow-level policy)."""

from __future__ import annotations

import pytest

from ffengine.ui.studio_service import normalize_notifications


def test_valid_policy_normalizes_and_dedupes():
    result = normalize_notifications(
        {
            "notify_on": ["failure", "FAILURE", "success"],
            "notify_emails": ["ops@bank.example", "ops@bank.example", "dev@bank.example"],
            "notify_conn_id": " smtp_default ",
        }
    )
    assert result == {
        "notify_on": ["failure", "success"],
        "notify_emails": ["ops@bank.example", "dev@bank.example"],
        "notify_conn_id": "smtp_default",
    }


def test_emails_accept_delimited_string():
    result = normalize_notifications(
        {
            "notify_on": ["failure"],
            "notify_emails": "a@x.com, b@y.com; c@z.com",
            "notify_conn_id": "smtp_default",
        }
    )
    assert result["notify_emails"] == ["a@x.com", "b@y.com", "c@z.com"]


def test_none_and_empty_are_disabled():
    assert normalize_notifications(None) is None
    assert normalize_notifications({}) is None
    assert (
        normalize_notifications(
            {"notify_on": [], "notify_emails": [], "notify_conn_id": ""}
        )
        is None
    )


def test_invalid_trigger_rejected():
    with pytest.raises(ValueError, match="Invalid notify_on trigger"):
        normalize_notifications(
            {
                "notify_on": ["failure", "bogus"],
                "notify_emails": ["a@x.com"],
                "notify_conn_id": "smtp_default",
            }
        )


def test_deadline_trigger_rejected_this_slice():
    # deadline is intentionally out of the Community v1 slice.
    with pytest.raises(ValueError, match="Invalid notify_on trigger"):
        normalize_notifications(
            {
                "notify_on": ["deadline"],
                "notify_emails": ["a@x.com"],
                "notify_conn_id": "smtp_default",
            }
        )


def test_enabled_without_emails_rejected():
    with pytest.raises(ValueError, match="at least one recipient"):
        normalize_notifications(
            {"notify_on": ["failure"], "notify_emails": [], "notify_conn_id": "smtp"}
        )


def test_bad_email_rejected():
    with pytest.raises(ValueError, match="Invalid email address"):
        normalize_notifications(
            {
                "notify_on": ["failure"],
                "notify_emails": ["not-an-email"],
                "notify_conn_id": "smtp_default",
            }
        )


def test_missing_conn_id_rejected():
    with pytest.raises(ValueError, match="notify_conn_id"):
        normalize_notifications(
            {
                "notify_on": ["failure"],
                "notify_emails": ["a@x.com"],
                "notify_conn_id": "",
            }
        )


def test_notify_on_must_be_list():
    with pytest.raises(ValueError, match="notify_on must be a list"):
        normalize_notifications(
            {
                "notify_on": "failure",
                "notify_emails": ["a@x.com"],
                "notify_conn_id": "smtp_default",
            }
        )


def test_non_dict_rejected():
    with pytest.raises(ValueError, match="notifications must be an object"):
        normalize_notifications(["failure"])
