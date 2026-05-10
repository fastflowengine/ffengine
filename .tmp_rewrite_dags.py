import ast
import pprint
import re
from pathlib import Path

import yaml

files = [
    Path('dags/webhook/whk/main/main/webhook_whk_main_main_group_1_dag.py'),
    Path('dags/webhook/whk/level1/src_to_stg/webhook_whk_level1_src_to_stg_group_1_dag.py'),
    Path('dags/webhook/whk/level2/stg_to_whk/webhook_whk_level2_stg_to_whk_group_1_dag.py'),
    Path('dags/webhook/oc_epr/level1/source_to_stg/oc_epr_to_stg_level1_group_1_dag.py'),
    Path('dags/webhook/oc_epr/level1/source_to_stg/webhook_oc_epr_level1_source_to_stg_group_2_dag.py'),
    Path('dags/webhook/oc_epr/level2/stg_to_ocepr/webhook_oc_epr_level2_stg_to_ocepr_group_1_dag.py'),
]

for dag_path in files:
    text = dag_path.read_text(encoding='utf-8')
    m_cfg = re.search(r'CONFIG_PATH\s*=\s*Path\("(?P<p>[^"]+)"\)', text)
    m_id = re.search(r'DAG_ID\s*=\s*"(?P<p>[^"]+)"', text)
    m_tags = re.search(r'DAG_TAGS\s*=\s*(?P<p>\[[^\n]*\])', text)
    m_up = re.search(r'UPSTREAM_DAG_IDS\s*=\s*(?P<p>\[[^\n]*\])', text)
    if not (m_cfg and m_id and m_tags and m_up):
        raise SystemExit(f'parse failed: {dag_path}')

    cfg_opt = m_cfg.group('p')
    dag_id = m_id.group('p')
    dag_tags = ast.literal_eval(m_tags.group('p'))
    upstream_ids = ast.literal_eval(m_up.group('p'))

    cfg_local_path = Path(cfg_opt.replace('/opt/airflow/', '', 1))
    raw = yaml.safe_load(cfg_local_path.read_text(encoding='utf-8')) or {}
    if not isinstance(raw, dict):
        raise SystemExit(f'invalid yaml root: {cfg_local_path}')
    raw['__config_path'] = cfg_opt
    raw_literal = pprint.pformat(raw, width=100, sort_dicts=False)

    new_text = (
        '# generated_by: flow_studio\n'
        'from pathlib import Path\n\n'
        'from ffengine.airflow.generated_factory import build_generated_dag\n\n'
        f'CONFIG_PATH = Path("{cfg_opt}")\n'
        f'DAG_ID = "{dag_id}"\n'
        f'DAG_TAGS = {repr(list(dag_tags))}\n'
        f'UPSTREAM_DAG_IDS = {repr(list(upstream_ids))}\n'
        f'RAW_CONFIG = {raw_literal}\n\n'
        'dag = build_generated_dag(\n'
        '    dag_id=DAG_ID,\n'
        '    dag_tags=DAG_TAGS,\n'
        '    upstream_dag_ids=UPSTREAM_DAG_IDS,\n'
        '    raw_config_snapshot=RAW_CONFIG,\n'
        ')\n'
    )
    dag_path.write_text(new_text, encoding='utf-8')
    print('updated', dag_path)
