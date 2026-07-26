"""F1.3b — named mail templates: rendering, storage, validation, secret-free."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from ffengine.airflow.notification_template import (
    DEFAULT_TEMPLATE,
    DEFAULT_TEMPLATE_NAME,
    TEMPLATES_VARIABLE_KEY,
    delete_template,
    load_template,
    load_templates,
    render_template,
    sample_meta,
    save_template,
)

_VAR = "airflow.models.Variable"


# ---- rendering: status-first default -------------------------------------

def test_default_subject_is_status_first_without_run():
    subject, html = render_template(DEFAULT_TEMPLATE, sample_meta("failure"), "FAILED")
    assert subject.startswith("FAILED ")
    assert "manual__" not in subject  # run suffix dropped
    assert subject == "FAILED [FFEngine] DAG orders_dag"
    assert html.startswith("<h3>FAILED")  # first body line leads with status


def test_default_failure_has_error_but_no_secret():
    _, html = render_template(DEFAULT_TEMPLATE, sample_meta("failure"), "FAILED")
    assert "ConnectionError" in html
    assert "batch write to target failed" in html
    assert "hidden" not in html  # raw details never rendered (INV-5)
    assert "no data rows" in html.lower()  # footer always appended


def test_default_success_has_no_error_rows():
    subject, html = render_template(DEFAULT_TEMPLATE, sample_meta("success"), "SUCCEEDED")
    assert subject.startswith("SUCCEEDED ")
    assert "Error message" not in html


# ---- rendering: custom template safety -----------------------------------

def test_custom_placeholders_and_escaping():
    meta = dict(sample_meta("failure"))
    meta["dag_id"] = "<b>x</b>"
    tpl = {
        "subject": "{{status}} :: {{dag_id}}",
        "html_body": "<p>{{status}} dag={{dag_id}} rows={{rows}} nope={{nope}}</p>",
    }
    subject, html = render_template(tpl, meta, "FAILED")
    assert subject == "FAILED :: <b>x</b>"  # subject is plain text (not escaped)
    assert "&lt;b&gt;x&lt;/b&gt;" in html  # body value escaped
    assert "rows=1234" in html
    assert "{{nope}}" in html  # unknown placeholder left literal


def test_subject_strips_newlines_header_injection_guard():
    tpl = {"subject": "{{status}}\r\nBcc: evil@x.com", "html_body": "<p>hi</p>"}
    subject, _ = render_template(tpl, sample_meta("success"), "SUCCEEDED")
    assert "\n" not in subject and "\r" not in subject


def test_no_error_details_placeholder_exists():
    # A template author cannot surface raw DB details — there is no such
    # placeholder, so it stays literal and no secret leaks.
    tpl = {"subject": "{{status}}", "html_body": "<p>{{error_details}} {{sql_preview}}</p>"}
    _, html = render_template(tpl, sample_meta("failure"), "FAILED")
    assert "{{error_details}}" in html
    assert "hidden" not in html


def test_render_never_raises_falls_back():
    # non-dict template ⇒ default is rendered, no exception
    subject, html = render_template(None, sample_meta("failure"), "FAILED")
    assert subject.startswith("FAILED")
    assert "<h3>" in html


# ---- storage (mocked Airflow Variable) -----------------------------------

def test_load_templates_merges_builtin_default():
    with patch(_VAR) as var:
        var.get.return_value = {"Custom": {"subject": "s", "html_body": "b"}}
        templates = load_templates()
    assert "Custom" in templates
    assert DEFAULT_TEMPLATE_NAME in templates  # built-in always present


def test_load_templates_default_on_missing_or_malformed():
    for bad in (None, "not-a-dict", {"Bad": {"subject": 1}}):
        with patch(_VAR) as var:
            var.get.return_value = bad
            templates = load_templates()
        assert list(templates.keys()) == [DEFAULT_TEMPLATE_NAME]


def test_load_template_unknown_falls_back_to_default():
    with patch(_VAR) as var:
        var.get.return_value = {"Custom": {"subject": "s", "html_body": "b"}}
        assert load_template("Custom")["subject"] == "s"
        assert load_template("nope") == DEFAULT_TEMPLATE
        assert load_template("") == DEFAULT_TEMPLATE


def test_save_template_writes_variable():
    with patch(_VAR) as var:
        var.get.return_value = {}
        save_template("Banka-Kritik", "{{status}} DAG {{dag_id}}", "<p>{{status}}</p>")
        var.set.assert_called_once()
    args, kwargs = var.set.call_args
    assert args[0] == TEMPLATES_VARIABLE_KEY
    assert "Banka-Kritik" in args[1]
    assert kwargs.get("serialize_json") is True


@pytest.mark.parametrize(
    "name,subject,body,match",
    [
        ("", "s", "b", "Template name"),
        ("bad/name", "s", "b", "Template name"),
        ("ok", "", "b", "subject cannot be empty"),
        ("ok", "s", "", "html_body cannot be empty"),
    ],
)
def test_save_template_validation(name, subject, body, match):
    with patch(_VAR):
        with pytest.raises(ValueError, match=match):
            save_template(name, subject, body)


def test_delete_template_default_resets_to_builtin():
    with patch(_VAR) as var:
        var.get.return_value = {"Default": {"subject": "x", "html_body": "y"}}
        delete_template("Default")
        var.set.assert_called_once()
    # after delete, the stored dict no longer has Default; load re-adds built-in
    with patch(_VAR) as var:
        var.get.return_value = {}
        assert load_template("Default") == DEFAULT_TEMPLATE
