# generated_by: etl_studio
from ffengine.airflow.dag_generator import register_dags

register_dags(
    "/opt/airflow/projects/webhook/whk/level1/src_to_stg",
    globals(),
    dag_prefix="ffengine",
)
