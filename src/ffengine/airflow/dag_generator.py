"""
C07 — Otomatik DAG üretici.

YAML config dizinini tarayarak her task için Airflow DAG nesnesi oluşturur.
"""

import logging
import os
from pathlib import Path

_log = logging.getLogger(__name__)


def generate_dags(
    config_dir: str,
    dag_prefix: str = "ffengine",
    schedule: str | None = None,
    tags: list[str] | None = None,
) -> dict:
    """
    config_dir altındaki YAML dosyalarını tarar ve her task için DAG üretir.

    Parameters
    ----------
    config_dir   : YAML config dosyalarının bulunduğu dizin.
    dag_prefix   : DAG ID prefix'i (varsayılan: "ffengine").
    schedule     : Airflow schedule ifadesi (varsayılan: None → manual).
    tags         : DAG etiketleri.

    Returns
    -------
    dict[str, DAG] : {dag_id: DAG} eşlemesi.
    """
    from datetime import datetime

    import yaml

    try:
        from airflow import DAG
    except ImportError:
        _log.warning("Airflow kurulu değil — generate_dags atlandi.")
        return {}

    config_path = Path(config_dir)
    if not config_path.is_dir():
        _log.warning("Config dizini bulunamadı: %s", config_dir)
        return {}

    effective_tags = tags or ["ffengine", "auto-generated"]
    dags: dict = {}

    for yaml_file in sorted(config_path.glob("*.yaml")):
        try:
            raw = yaml.safe_load(yaml_file.read_text(encoding="utf-8"))
        except Exception as exc:
            _log.warning("YAML parse hatası, atlanıyor: %s — %s", yaml_file.name, exc)
            continue

        if not isinstance(raw, dict):
            _log.warning("Geçersiz YAML yapısı, atlanıyor: %s", yaml_file.name)
            continue

        tasks = raw.get("flow_tasks", [])
        if not isinstance(tasks, list):
            continue

        source_conn_id = raw.get("source_db_var", "ffengine_source")
        target_conn_id = raw.get("target_db_var", "ffengine_target")

        for task in tasks:
            if not isinstance(task, dict):
                continue

            tg_id = task.get("task_group_id")
            if not tg_id:
                continue

            dag_id = f"{dag_prefix}_{yaml_file.stem}_{tg_id}"

            dag = DAG(
                dag_id=dag_id,
                schedule=schedule,
                start_date=datetime(2023, 1, 1),
                catchup=False,
                tags=effective_tags,
            )

            from ffengine.airflow.operator import FFEngineOperator

            FFEngineOperator(
                config_path=str(yaml_file),
                task_group_id=tg_id,
                source_conn_id=source_conn_id,
                target_conn_id=target_conn_id,
                task_id=f"run_{tg_id}",
                dag=dag,
            )

            dags[dag_id] = dag

    _log.info("generate_dags: %d DAG üretildi (%s)", len(dags), config_dir)
    return dags


def register_dags(
    config_dir: str,
    globals_dict: dict,
    **kwargs,
) -> None:
    """
    generate_dags() çıktısını globals() dict'ine kaydeder.

    Kullanım (dags/*.py içinde):
        from ffengine.airflow.dag_generator import register_dags
        register_dags("/opt/airflow/configs", globals())
    """
    dags = generate_dags(config_dir, **kwargs)
    globals_dict.update(dags)
