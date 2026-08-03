"""
F2.1 — edition gate (INV-8). Generic FFENGINE_EDITION flag; Community never
imports/names the Enterprise package. Maps to T-F2.1-5 (edition).
"""

from ffengine.core.edition import edition, is_enterprise_enabled


def test_edition_default_is_community(monkeypatch):
    monkeypatch.delenv("FFENGINE_EDITION", raising=False)
    assert edition() == "community"
    assert is_enterprise_enabled() is False


def test_is_enterprise_enabled_when_flag_set(monkeypatch):
    monkeypatch.setenv("FFENGINE_EDITION", "enterprise")
    assert is_enterprise_enabled() is True


def test_edition_is_case_insensitive_and_stripped(monkeypatch):
    monkeypatch.setenv("FFENGINE_EDITION", "  Enterprise ")
    assert edition() == "enterprise"
    assert is_enterprise_enabled() is True


def test_other_values_do_not_enable_enterprise(monkeypatch):
    monkeypatch.setenv("FFENGINE_EDITION", "pro")
    assert is_enterprise_enabled() is False
