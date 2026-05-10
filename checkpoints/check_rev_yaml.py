import yaml
from pathlib import Path
root=Path('projects/webhook/whk/level1/src_to_stg/.flow_studio_history/webhook_whk_level1_src_to_stg_group_1_dag')
for d in sorted([p for p in root.iterdir() if p.is_dir()]):
    cfg=d/'config.yaml'
    try:
        obj=yaml.safe_load(cfg.read_text(encoding='utf-8'))
        print(d.name, 'YAML_OK', type(obj).__name__)
    except Exception as e:
        print(d.name, 'YAML_ERR', type(e).__name__, str(e))
