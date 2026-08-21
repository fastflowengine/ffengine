"""F7.1 — Airflow-native task retry (Community).

Retry MOTORU yazilmaz (ARCH-09): config'teki `retry` blogu Airflow'un kendi
`default_args.retries` / `retry_delay` alanlarina cevrilir ve gerisini
scheduler yapar. Boylece yeniden deneme davranisi Airflow UI'da (try_number,
"Clear" vb.) yerlesik gorunur ve operatorde paralel bir durum makinesi
tasimayiz.

Zero-diff: `retry` blogu yoksa **bos dict** doner ve DAG cagrisina hicbir
`default_args` eklenmez -- retry kullanmayan mevcut DAG'lar bayt-ayni kalir
(ARCH-11).
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from ffengine.config.schema import normalize_retry


def build_retry_kwargs(raw_retry: Any) -> dict[str, Any]:
    """`retry` blogunu ``DAG(**kwargs)`` icin Airflow argumanlarina cevirir.

    Dogrulama tek merkezden (`config.schema.normalize_retry`) gecer:
    DAG-parse yolunda gecersiz bir blok sessizce yok sayilmaz, fail-loud
    yukselir (ARCH-06).
    """
    normalized = normalize_retry(raw_retry)
    if not normalized:
        return {}
    return {
        "default_args": {
            "retries": normalized["retries"],
            "retry_delay": timedelta(seconds=normalized["delay_seconds"]),
        }
    }
