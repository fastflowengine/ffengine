import os
from pathlib import Path
from ffengine.ui import studio_service as ss

repo = Path(r'c:/fast-flow/FFEngineCommunity')
os.environ['FFENGINE_STUDIO_DAG_ROOT'] = str((repo / 'dags').resolve())
os.environ['FFENGINE_STUDIO_PROJECTS_ROOT'] = str((repo / 'projects').resolve())
os.environ['FFENGINE_STUDIO_PROMOTE_VERIFY_PARSE'] = '0'

dag_id = 'webhook_whk_level1_src_to_stg_group_1_dag'

dag_path = ss._find_studio_dag_file_by_id(dag_id)
if dag_path is None:
    raise SystemExit(f'DAG not found: {dag_id}')
config_path = ss._extract_config_path_from_dag_source(dag_path)
flow_dir = config_path.resolve().parent

manifest = ss._save_bundle_as_revision(
    flow_dir=flow_dir,
    dag_id=dag_id,
    dag_path=dag_path,
    config_path=config_path,
    source=ss.REVISION_SOURCE_UPDATE,
    actor='codex_manual_snapshot',
)
new_rev = str(manifest.get('revision_id') or '')
if not new_rev:
    raise SystemExit('Failed to create new revision id')

promoted = ss.promote_dag_revision(dag_id=dag_id, revision_id=new_rev)

print('NEW_REV', new_rev)
print('PROMOTE_OK', promoted.get('promoted_revision_id'), promoted.get('active_revision_id'), promoted.get('no_op'))
print('WARNINGS', promoted.get('warnings'))
