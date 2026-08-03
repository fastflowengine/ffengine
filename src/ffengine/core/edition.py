"""
F2.1 — Edition gate (generic; TAD v26.5 A6.3 + review).

Community must NOT import or name the Enterprise package. The edition gate is a
generic env flag (``FFENGINE_EDITION``) — a merely-importable fake module must
not unlock Enterprise behaviour. Actual capability (a registered bulk provider)
is checked separately via ``ffengine.pipeline.bulk_registry``.

Two-layer gating (INV-8):
  * UI hides Enterprise-only fields when the edition is not enterprise.
  * Backend rejects Enterprise-only config with **422** (config validation) when
    the edition is not enabled. (403 = RBAC/authz is F2.2; F2.1 has no RBAC, so
    the edition gate uses 422 unambiguously.)
"""

from __future__ import annotations

import os

EDITION_ENV = "FFENGINE_EDITION"
ENTERPRISE = "enterprise"


def edition() -> str:
    """Current edition string from the environment (default 'community')."""
    return str(os.environ.get(EDITION_ENV, "community")).strip().lower()


def is_enterprise_enabled() -> bool:
    """True iff the deployment declares the Enterprise edition.

    Generic: reads only ``FFENGINE_EDITION``; Community never imports or names
    ``ffengine_enterprise``.
    """
    return edition() == ENTERPRISE
