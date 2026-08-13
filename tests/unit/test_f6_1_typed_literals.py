"""F6.1 dialect-aware typed SQL literal security tests."""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from ffengine.config.binding_resolver import BindingResolver
from ffengine.errors.exceptions import ConfigError


class PostgresDialect:
    pass


class MSSQLDialect:
    pass


class OracleDialect:
    pass


@pytest.mark.parametrize(
    "dialect,expected",
    [
        (PostgresDialect(), "'O''Brien ☃'"),
        (MSSQLDialect(), "N'O''Brien ☃'"),
        (OracleDialect(), "'O''Brien ☃'"),
    ],
)
def test_unicode_and_apostrophe_are_quoted_per_dialect(dialect, expected):
    assert BindingResolver()._to_sql_literal(
        "O'Brien ☃", where_dialect=dialect
    ) == expected


@pytest.mark.parametrize(
    "dialect,expected_true,expected_false",
    [
        (PostgresDialect(), "TRUE", "FALSE"),
        (MSSQLDialect(), "1", "0"),
        (OracleDialect(), "1", "0"),
    ],
)
def test_boolean_literal_is_dialect_aware(dialect, expected_true, expected_false):
    resolver = BindingResolver()
    assert resolver._to_sql_literal(True, where_dialect=dialect) == expected_true
    assert resolver._to_sql_literal(False, where_dialect=dialect) == expected_false


def test_numeric_and_null_are_unquoted_and_non_finite_is_rejected():
    resolver = BindingResolver()
    assert resolver._to_sql_literal(None) == "NULL"
    assert resolver._to_sql_literal(Decimal("123.450")) == "123.450"
    assert resolver._to_sql_literal(-7) == "-7"
    with pytest.raises(ConfigError, match="NaN/Inf"):
        resolver._to_sql_literal(float("nan"))


@pytest.mark.parametrize(
    "dialect,date_expected,timestamp_expected",
    [
        (
            PostgresDialect(),
            "DATE '2026-01-02'",
            "TIMESTAMPTZ '2026-01-02 00:04:05.123456+00:00'",
        ),
        (
            MSSQLDialect(),
            "CAST(N'2026-01-02' AS date)",
            "CAST(N'2026-01-02 00:04:05.123456' AS datetime2(6))",
        ),
        (
            OracleDialect(),
            "DATE '2026-01-02'",
            "TIMESTAMP '2026-01-02 00:04:05.123456'",
        ),
    ],
)
def test_date_and_timestamp_literals_are_dialect_aware(
    dialect, date_expected, timestamp_expected
):
    resolver = BindingResolver()
    instant = datetime(
        2026,
        1,
        2,
        3,
        4,
        5,
        123456,
        tzinfo=timezone(timedelta(hours=3)),
    )
    assert resolver._to_sql_literal(date(2026, 1, 2), where_dialect=dialect) == (
        date_expected
    )
    assert resolver._to_sql_literal(instant, where_dialect=dialect) == (
        timestamp_expected
    )


def test_injection_payload_remains_inside_one_string_literal():
    payload = "x'; DROP TABLE accounts; --"
    rendered = BindingResolver()._to_sql_literal(
        payload, where_dialect=MSSQLDialect()
    )

    assert rendered == "N'x''; DROP TABLE accounts; --'"
