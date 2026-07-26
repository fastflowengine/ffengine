"""
F1.2 - Controlled push-down expression model.

Parses a constrained expression grammar into an AST, validates it against a
fixed function whitelist and the mapping's physical source columns, and renders
dialect SQL. It is NOT a raw-SQL channel: semicolons, SQL comments, subqueries,
DDL/DML keywords, schema/table-qualified identifiers and derived-column chaining
are rejected before any SQL is produced (fail-loud).

Canonical vocabulary (dialect-agnostic; rendered per dialect):
  concat, coalesce, cast(x as type), upper, lower, trim, substring, replace,
  current_date, current_timestamp, arithmetic (+ - * /), plus source-column
  references and string/number/boolean/null literals.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable

from ffengine.errors.exceptions import MappingError

# Regular functions (comma-separated args). cast/current_date/current_timestamp
# are handled as dedicated grammar productions, not generic calls.
_ARITY: dict[str, tuple[int, int | None]] = {
    "concat": (1, None),
    "coalesce": (1, None),
    "upper": (1, 1),
    "lower": (1, 1),
    "trim": (1, 1),
    "substring": (2, 3),
    "replace": (3, 3),
}
_NOW = {"current_date", "current_timestamp"}
_KEYWORDS = {"as", "true", "false", "null"} | _NOW
# Defense-in-depth: reject these even in column-ref position for a clear message.
_DENY = {
    "select", "insert", "update", "delete", "drop", "alter", "create",
    "truncate", "merge", "exec", "execute", "union", "from", "where", "join",
    "into", "values", "grant", "revoke", "call",
}


# --- AST -------------------------------------------------------------------

@dataclass(frozen=True)
class Col:
    name: str


@dataclass(frozen=True)
class Str:
    value: str


@dataclass(frozen=True)
class Num:
    text: str


@dataclass(frozen=True)
class Bool:
    value: bool


@dataclass(frozen=True)
class Null:
    pass


@dataclass(frozen=True)
class Func:
    name: str
    args: tuple


@dataclass(frozen=True)
class Cast:
    inner: Any
    type_literal: str


@dataclass(frozen=True)
class Now:
    kind: str


@dataclass(frozen=True)
class BinOp:
    op: str
    left: Any
    right: Any


@dataclass(frozen=True)
class UnOp:
    op: str
    operand: Any


Expr = Any


# --- Tokenizer -------------------------------------------------------------

_TOKEN_RE = re.compile(
    r"""\s+
      | (?P<num>\d+(?:\.\d+)?)
      | (?P<str>'(?:[^']|'')*')
      | (?P<ident>[A-Za-z_][A-Za-z0-9_]*)
      | (?P<op>[(),+\-*/])""",
    re.VERBOSE,
)
_INJECTION = (";", "--", "/*", "*/", "\\")


def _tokenize(text: str) -> list[tuple[str, str]]:
    for bad in _INJECTION:
        if bad in text:
            raise MappingError(
                f"Expression rejected: illegal sequence {bad!r} is not allowed."
            )
    tokens: list[tuple[str, str]] = []
    pos = 0
    while pos < len(text):
        m = _TOKEN_RE.match(text, pos)
        if not m:
            raise MappingError(
                f"Expression rejected: illegal character {text[pos]!r} "
                f"at position {pos}."
            )
        pos = m.end()
        if m.lastgroup is None:  # whitespace
            continue
        tokens.append((m.lastgroup, m.group()))
    return tokens


# --- Parser (recursive descent) --------------------------------------------

class _Parser:
    def __init__(self, tokens: list[tuple[str, str]]):
        self.toks = tokens
        self.i = 0

    def _peek(self) -> tuple[str, str] | None:
        return self.toks[self.i] if self.i < len(self.toks) else None

    def _next(self) -> tuple[str, str]:
        tok = self._peek()
        if tok is None:
            raise MappingError("Expression rejected: unexpected end of input.")
        self.i += 1
        return tok

    def _expect_op(self, op: str) -> None:
        kind, val = self._next()
        if kind != "op" or val != op:
            raise MappingError(f"Expression rejected: expected {op!r}, got {val!r}.")

    def parse(self) -> Expr:
        node = self._add()
        if self._peek() is not None:
            raise MappingError(
                f"Expression rejected: trailing tokens near {self._peek()[1]!r}."
            )
        return node

    def _add(self) -> Expr:
        node = self._mul()
        while (t := self._peek()) and t[0] == "op" and t[1] in "+-":
            self.i += 1
            node = BinOp(t[1], node, self._mul())
        return node

    def _mul(self) -> Expr:
        node = self._unary()
        while (t := self._peek()) and t[0] == "op" and t[1] in "*/":
            self.i += 1
            node = BinOp(t[1], node, self._unary())
        return node

    def _unary(self) -> Expr:
        t = self._peek()
        if t and t[0] == "op" and t[1] in "+-":
            self.i += 1
            return UnOp(t[1], self._unary())
        return self._primary()

    def _primary(self) -> Expr:
        kind, val = self._next()
        if kind == "num":
            return Num(val)
        if kind == "str":
            return Str(val[1:-1].replace("''", "'"))
        if kind == "op" and val == "(":
            node = self._add()
            self._expect_op(")")
            return node
        if kind == "ident":
            return self._ident(val)
        raise MappingError(f"Expression rejected: unexpected token {val!r}.")

    def _ident(self, name: str) -> Expr:
        low = name.lower()
        nxt = self._peek()
        is_call = nxt is not None and nxt == ("op", "(")
        if not is_call:
            if low == "null":
                return Null()
            if low in ("true", "false"):
                return Bool(low == "true")
            if low in _NOW:
                return Now(low)
            if low in _KEYWORDS or low in _DENY:
                raise MappingError(
                    f"Expression rejected: reserved keyword {name!r} "
                    "cannot be used as a column."
                )
            return Col(name)
        if low == "cast":
            return self._cast()
        if low in _NOW:  # current_date()/current_timestamp()
            self._expect_op("(")
            self._expect_op(")")
            return Now(low)
        if low not in _ARITY:
            raise MappingError(
                f"Expression rejected: unsupported function {name!r}."
            )
        return Func(low, tuple(self._arglist()))

    def _cast(self) -> Expr:
        self._expect_op("(")
        inner = self._add()
        kind, val = self._next()
        if kind != "ident" or val.lower() != "as":
            raise MappingError("Expression rejected: cast requires 'AS <type>'.")
        type_literal = self._typename()
        self._expect_op(")")
        return Cast(inner, type_literal)

    def _typename(self) -> str:
        kind, base = self._next()
        if kind != "ident":
            raise MappingError("Expression rejected: cast target type is invalid.")
        out = base
        if (t := self._peek()) and t == ("op", "("):
            self.i += 1
            parts: list[str] = []
            while True:
                k2, v2 = self._next()
                if k2 != "num":
                    raise MappingError("Expression rejected: type params must be numeric.")
                parts.append(v2)
                nk, nv = self._next()
                if nk == "op" and nv == ")":
                    break
                if not (nk == "op" and nv == ","):
                    raise MappingError("Expression rejected: malformed type params.")
            out += "(" + ",".join(parts) + ")"
        return out

    def _arglist(self) -> list[Expr]:
        self._expect_op("(")
        args: list[Expr] = []
        if self._peek() == ("op", ")"):
            self.i += 1
            return args
        while True:
            args.append(self._add())
            kind, val = self._next()
            if kind == "op" and val == ")":
                return args
            if not (kind == "op" and val == ","):
                raise MappingError(
                    f"Expression rejected: expected ',' or ')', got {val!r}."
                )


def parse(text: str) -> Expr:
    """Parse expression text into a validated-syntax AST (fail-loud)."""
    raw = str(text or "").strip()
    if not raw:
        raise MappingError("Expression rejected: empty expression.")
    return _Parser(_tokenize(raw)).parse()


# --- Validation ------------------------------------------------------------

def validate(expr: Expr, allowed_columns: set[str]) -> None:
    """Reject unknown columns/functions and enforce arities (fail-loud)."""
    if isinstance(expr, Col):
        if expr.name not in allowed_columns:
            raise MappingError(
                f"Expression references unknown source column '{expr.name}'."
            )
    elif isinstance(expr, Func):
        lo, hi = _ARITY[expr.name]
        n = len(expr.args)
        if n < lo or (hi is not None and n > hi):
            raise MappingError(
                f"Expression function '{expr.name}' got {n} args (expects "
                f"{lo}{'' if hi == lo else '+' if hi is None else f'..{hi}'})."
            )
        for a in expr.args:
            validate(a, allowed_columns)
    elif isinstance(expr, Cast):
        validate(expr.inner, allowed_columns)
    elif isinstance(expr, BinOp):
        validate(expr.left, allowed_columns)
        validate(expr.right, allowed_columns)
    elif isinstance(expr, UnOp):
        validate(expr.operand, allowed_columns)
    # Col/Str/Num/Bool/Null/Now leaves already handled or need no check.


def column_refs(expr: Expr) -> list[str]:
    """Ordered source-column references (with duplicates) = bind order."""
    out: list[str] = []
    _collect(expr, out)
    return out


def _collect(expr: Expr, out: list[str]) -> None:
    if isinstance(expr, Col):
        out.append(expr.name)
    elif isinstance(expr, Func):
        for a in expr.args:
            _collect(a, out)
    elif isinstance(expr, Cast):
        _collect(expr.inner, out)
    elif isinstance(expr, BinOp):
        _collect(expr.left, out)
        _collect(expr.right, out)
    elif isinstance(expr, UnOp):
        _collect(expr.operand, out)


def compile_expression(text: str, allowed_columns: set[str]) -> Expr:
    """parse + validate convenience; returns the AST or raises MappingError."""
    expr = parse(text)
    validate(expr, allowed_columns)
    return expr


# --- Rendering -------------------------------------------------------------

class ExprOps:
    """
    Dialect rendering hooks (PostgreSQL-style defaults). MSSQL/Oracle subclass
    the few differences. `mode` is 'bind' (executemany VALUES: column -> next
    placeholder via `alloc`) or 'colref' (staging SELECT: column -> quoted ref).
    """

    def __init__(
        self,
        *,
        mode: str,
        alloc: Callable[[str], str] | None = None,
        quote: Callable[[str], str] | None = None,
    ):
        if mode not in ("bind", "colref"):
            raise MappingError(f"Unknown expression render mode: {mode!r}.")
        self.mode = mode
        self._alloc = alloc
        self._quote = quote or (lambda n: n)

    def column(self, name: str) -> str:
        if self.mode == "bind":
            return self._alloc(name)
        return self._quote(name)

    def string_literal(self, value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    def boolean(self, value: bool) -> str:
        return "TRUE" if value else "FALSE"

    def now(self, kind: str) -> str:
        return "CURRENT_DATE" if kind == "current_date" else "CURRENT_TIMESTAMP"

    def concat(self, parts: list[str]) -> str:
        return "(" + " || ".join(parts) + ")"

    def func(self, name: str, args: list[str]) -> str:
        return f"{name.upper()}({', '.join(args)})"

    def cast(self, inner: str, type_literal: str) -> str:
        return f"CAST({inner} AS {type_literal.upper()})"


class MSSQLExprOps(ExprOps):
    def boolean(self, value: bool) -> str:
        return "1" if value else "0"

    def now(self, kind: str) -> str:
        return "CAST(GETDATE() AS DATE)" if kind == "current_date" else "SYSDATETIME()"

    def concat(self, parts: list[str]) -> str:
        return f"CONCAT({', '.join(parts)})"


class OracleExprOps(ExprOps):
    def boolean(self, value: bool) -> str:
        return "1" if value else "0"

    def now(self, kind: str) -> str:
        return "TRUNC(SYSDATE)" if kind == "current_date" else "SYSTIMESTAMP"

    def func(self, name: str, args: list[str]) -> str:
        canonical = "SUBSTR" if name.lower() == "substring" else name.upper()
        return f"{canonical}({', '.join(args)})"


_OPS_BY_DIALECT = {"mssql": MSSQLExprOps, "oracle": OracleExprOps}


def ops_for(
    dialect_name: str,
    *,
    mode: str,
    alloc: Callable[[str], str] | None = None,
    quote: Callable[[str], str] | None = None,
) -> ExprOps:
    """Build the ExprOps for a dialect short name ('postgres'|'mssql'|'oracle')."""
    cls = _OPS_BY_DIALECT.get(str(dialect_name).lower(), ExprOps)
    return cls(mode=mode, alloc=alloc, quote=quote)


def render(expr: Expr, ops: ExprOps) -> str:
    """Render a validated AST to dialect SQL via `ops`."""
    if isinstance(expr, Col):
        return ops.column(expr.name)
    if isinstance(expr, Str):
        return ops.string_literal(expr.value)
    if isinstance(expr, Num):
        return expr.text
    if isinstance(expr, Bool):
        return ops.boolean(expr.value)
    if isinstance(expr, Null):
        return "NULL"
    if isinstance(expr, Now):
        return ops.now(expr.kind)
    if isinstance(expr, Func):
        rendered = [render(a, ops) for a in expr.args]
        if expr.name == "concat":
            return ops.concat(rendered)
        return ops.func(expr.name, rendered)
    if isinstance(expr, Cast):
        return ops.cast(render(expr.inner, ops), expr.type_literal)
    if isinstance(expr, BinOp):
        return f"({render(expr.left, ops)} {expr.op} {render(expr.right, ops)})"
    if isinstance(expr, UnOp):
        return f"({expr.op}{render(expr.operand, ops)})"
    raise MappingError(f"Expression rejected: unrenderable node {type(expr).__name__}.")
