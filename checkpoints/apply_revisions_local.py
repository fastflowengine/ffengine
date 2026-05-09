from pathlib import Path
from ffengine.ui import studio_service as ss

repo = Path(r'c:/fast-flow/FFEngineCommunity')
dag_id = 'webhook_whk_level1_src_to_stg_group_1_dag'
flow_dir = repo / 'projects' / 'webhook' / 'whk' / 'level1' / 'src_to_stg'
dag_path = repo / 'dags' / 'webhook' / 'whk' / 'level1' / 'src_to_stg' / f'{dag_id}.py'
config_path = flow_dir / 'webhook_whk_level1_src_to_stg_group_1.yaml'
history_root = flow_dir / ss.STUDIO_HISTORY_DIR_NAME / dag_id

for rid in ['rev_000003','rev_000005','rev_000007']:
    rev_dir = history_root / rid
    bundle = ss._load_bundle_from_revision(rev_dir)
    ss._apply_bundle_to_active(
        flow_dir=flow_dir,
        dag_path=dag_path,
        config_path=config_path,
        bundle=bundle,
    )
    print('APPLIED', rid)
print('DONE')
