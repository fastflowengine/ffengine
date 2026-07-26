"""F1.2 - guard: transformer.py stays passthrough (enrichment is push-down, not in-flight)."""

from __future__ import annotations

import inspect

from ffengine.pipeline.transformer import Transformer


def test_apply_is_identity_when_rules_none():
    rows = [(1, "a"), (2, "b")]
    assert Transformer().apply(rows, columns=[], rules=None) is rows


def test_apply_identity_with_empty_rules():
    rows = [(1, "a")]
    assert Transformer().apply(rows, columns=[{"name": "id"}], rules={}) is rows


def test_streamer_calls_transformer_with_rules_none():
    # The hot path must never enable in-flight compute (D3 / A2.2).
    src = inspect.getsource(
        __import__("ffengine.pipeline.streamer", fromlist=["stream"])
    )
    assert "transformer.apply(chunk, columns=[], rules=None)" in src
