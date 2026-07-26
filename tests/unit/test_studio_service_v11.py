"""F1.2 - Flow Studio v1.1 mapping accept / validate / round-trip."""

from __future__ import annotations

import textwrap

import pytest

from ffengine.ui import studio_service as ss

_V11 = textwrap.dedent(
    """\
    version: v1.1
    columns:
      - source_name: first_name
        target_name: first_name
        target_type: varchar(100)
      - target_name: full_name
        target_type: varchar(220)
        expression: concat(first_name, ' ', last_name)
    """
)


def test_v11_mapping_parses_and_preserves_expression():
    parsed = ss._parse_yaml_mapping_text(_V11, label="t")
    assert parsed["version"] == "v1.1"
    derived = parsed["columns"][1]
    assert derived["expression"] == "concat(first_name, ' ', last_name)"
    assert "source_name" not in derived


def test_v11_source_columns_include_expression_refs():
    parsed = ss._parse_yaml_mapping_text(_V11, label="t")
    assert ss._mapping_yaml_to_source_columns(parsed) == ["first_name", "last_name"]


def test_v11_expression_syntax_error_rejected_on_save():
    bad = textwrap.dedent(
        """\
        version: v1.1
        columns:
          - target_name: x
            target_type: varchar(10)
            expression: "concat(a; drop table t)"
        """
    )
    parsed = ss._parse_yaml_mapping_text(bad, label="t")
    with pytest.raises(ValueError, match="expression invalid"):
        ss._mapping_yaml_to_source_columns(parsed)


def test_v11_both_source_and_expression_rejected():
    bad = textwrap.dedent(
        """\
        version: v1.1
        columns:
          - source_name: email
            target_name: email
            target_type: varchar(80)
            expression: lower(email)
        """
    )
    with pytest.raises(ValueError, match="both"):
        ss._parse_yaml_mapping_text(bad, label="t")


def test_parse_mapping_columns_returns_structured_columns():
    out = ss.parse_mapping_columns(_V11)
    assert out["version"] == "v1.1"
    kinds = [
        ("expr" if c.get("expression") else "direct", c.get("target_name"))
        for c in out["columns"]
    ]
    assert kinds == [("direct", "first_name"), ("expr", "full_name")]


def test_parse_mapping_columns_lenient_empty():
    assert ss.parse_mapping_columns("")["columns"] == []
    assert ss.parse_mapping_columns("   ")["columns"] == []


def test_parse_mapping_columns_lenient_does_not_validate_xor():
    # Lenient load: both source_name + expression is not rejected here (strict
    # validation happens on save), so the row editor can load and repair it.
    both = textwrap.dedent(
        """\
        version: v1.1
        columns:
          - source_name: email
            target_name: email
            target_type: varchar(80)
            expression: lower(email)
        """
    )
    out = ss.parse_mapping_columns(both)
    assert out["columns"][0]["expression"] == "lower(email)"


def test_parse_mapping_columns_bad_yaml_rejected():
    with pytest.raises(ValueError, match="Invalid mapping YAML"):
        ss.parse_mapping_columns(": bad\n  broken:")
