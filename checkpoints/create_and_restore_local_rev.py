from pathlib import Path
from ffengine.ui import studio_service as ss

repo = Path(r'c:/fast-flow/FFEngineCommunity')
dag_id = 'webhook_whk_level1_src_to_stg_group_1_dag'
dag_path = repo / 'dags' / 'webhook' / 'whk' / 'level1' / 'src_to_stg' / f'{dag_id}.py'
config_path = repo / 'projects' / 'webhook' / 'whk' / 'level1' / 'src_to_stg' / 'webhook_whk_level1_src_to_stg_group_1.yaml'
flow_dir = config_path.parent

if not dag_path.is_file():
    raise SystemExit(f'missing dag: {dag_path}')
if not config_path.is_file():
    raise SystemExit(f'missing config: {config_path}')

manifest = ss._save_bundle_as_revision(
    flow_dir=flow_dir,
    dag_id=dag_id,
    dag_path=dag_path,
    config_path=config_path,
    source=ss.REVISION_SOURCE_UPDATE,
    actor='codex_manual_snapshot',
)
new_rev = str(manifest.get('revision_id') or '')
rev_dir = flow_dir / ss.STUDIO_HISTORY_DIR_NAME / dag_id / new_rev
bundle = ss._load_bundle_from_revision(rev_dir)
ss._apply_bundle_to_active(
    flow_dir=flow_dir,
    dag_path=dag_path,
    config_path=config_path,
    bundle=bundle,
)
print('NEW_REV', new_rev)
print('RESTORED_FROM', rev_dir.as_posix())
