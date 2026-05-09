import ast
from pathlib import Path
root=Path('projects/webhook/whk/level1/src_to_stg/.flow_studio_history/webhook_whk_level1_src_to_stg_group_1_dag')
for d in sorted([p for p in root.iterdir() if p.is_dir()]):
    dag_file=d/'dag.py'
    try:
        ast.parse(dag_file.read_text(encoding='utf-8'))
        print(d.name, 'OK')
    except Exception as e:
        print(d.name, 'ERR', type(e).__name__, str(e))
