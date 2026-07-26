"""F1.2 - strict mapping validation for v1.1 (XOR, explicit type, uniqueness)."""

from __future__ import annotations

import pytest

from ffengine.errors.exceptions import MappingError
from ffengine.mapping.type_contract import validate_mapping_object_strict

VERSIONS = frozenset({"v1", "v1.1"})


def _validate(mapping):
    return validate_mapping_object_strict(
        mapping, valid_versions=VERSIONS, error_cls=MappingError, context="t"
    )


def _v11(columns):
    return {"version": "v1.1", "columns": columns}


def test_direct_and_derived_columns_valid():
    mapping = _v11([
        {"source_name": "first_name", "target_name": "first_name", "target_type": "varchar(100)"},
        {"target_name": "full_name", "target_type": "varchar(220)",
         "expression": "concat(first_name, ' ', last_name)"},
        {"target_name": "load_date", "target_type": "date", "expression": "current_date"},
    ])
    assert _validate(mapping) is mapping


def test_both_source_name_and_expression_rejected():
    with pytest.raises(MappingError, match="both"):
        _validate(_v11([
            {"source_name": "email", "target_name": "email", "target_type": "varchar(80)",
             "expression": "lower(email)"},
        ]))


def test_neither_source_name_nor_expression_rejected():
    with pytest.raises(MappingError, match="exactly one"):
        _validate(_v11([{"target_name": "x", "target_type": "varchar(10)"}]))


def test_derived_requires_explicit_target_type():
    with pytest.raises(MappingError, match="target_type"):
        _validate(_v11([{"target_name": "x", "expression": "current_date"}]))


def test_expression_in_v1_rejected():
    with pytest.raises(MappingError, match="v1.1"):
        validate_mapping_object_strict(
            {"version": "v1", "columns": [
                {"target_name": "x", "target_type": "date", "expression": "current_date"}]},
            valid_versions=VERSIONS, error_cls=MappingError, context="t",
        )


def test_duplicate_target_name_rejected():
    with pytest.raises(MappingError, match="duplicate"):
        _validate(_v11([
            {"source_name": "a", "target_name": "dup", "target_type": "varchar(10)"},
            {"target_name": "dup", "target_type": "varchar(10)", "expression": "upper(a)"},
        ]))


def test_length_bearing_requires_explicit_length():
    with pytest.raises(MappingError, match="explicit length"):
        _validate(_v11([
            {"target_name": "full_name", "target_type": "varchar",
             "expression": "concat(first_name, last_name)"},
        ]))


def test_numeric_requires_precision_scale():
    with pytest.raises(MappingError, match="precision/scale"):
        _validate(_v11([
            {"target_name": "total", "target_type": "numeric", "expression": "amount"},
        ]))


def test_same_dialect_bare_length_accepted():
    # Same Connection Type: an unsized varchar is a lossless "max size"
    # passthrough (non-narrowing), so the explicit-length rule is waived.
    mapping = {
        "version": "v1",
        "source_dialect": "postgres",
        "target_dialect": "postgres",
        "columns": [
            {"source_name": "note", "target_name": "note", "target_type": "varchar"},
        ],
    }
    assert _validate(mapping) is mapping


def test_same_dialect_bare_numeric_accepted():
    mapping = {
        "version": "v1",
        "source_dialect": "postgres",
        "target_dialect": "postgres",
        "columns": [
            {"source_name": "amount", "target_name": "amount", "target_type": "numeric"},
        ],
    }
    assert _validate(mapping) is mapping


def test_cross_dialect_bare_numeric_still_rejected():
    # Different Connection Type: "bare" is not a guaranteed max, so INV-1 still
    # fails loud.
    with pytest.raises(MappingError, match="precision/scale"):
        _validate({
            "version": "v1",
            "source_dialect": "postgres",
            "target_dialect": "oracle",
            "columns": [
                {"source_name": "amount", "target_name": "amount", "target_type": "numeric"},
            ],
        })


def test_v1_direct_mapping_still_valid():
    mapping = {"version": "v1", "columns": [
        {"source_name": "a", "target_name": "a", "target_type": "varchar(10)"}]}
    assert validate_mapping_object_strict(
        mapping, valid_versions=VERSIONS, error_cls=MappingError, context="t"
    ) is mapping
