# Airflow Pattern

## 3 Fazlı DAG Pattern

### Faz Sırası
```
plan_partitions → prepare_target → run_partition.expand()
```

### Tam Şablon
```python
from airflow.decorators import dag, task, task_group
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from ffengine.tools.send_email_custom import send_failure_email
from datetime import datetime

default_args = {
    "owner": "ffengine",
    "retries": 1,
    "on_failure_callback": send_failure_email,  # C10 — email notification
}

with DAG(
    dag_id="whk_level1_src_to_stg",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    tags=["whk", "level1", "src_to_stg"],
    catchup=False,
) as dag:

    for etl_task in config["etl_tasks"]:
        with task_group(group_id=etl_task["task_group_id"]) as tg:

            @task
            def plan_partitions(task_cfg: dict) -> list[dict]:
                from ffengine.partition.partitioner import Partitioner
                from ffengine.config.binding_resolver import BindingResolver
                resolved = BindingResolver(task_cfg).resolve()
                return Partitioner(task_cfg).generate(resolved)
                # XCom çıktı formatı:
                # [{"part_id": 0, "where": "id >= 0 AND id < 50000"},
                #  {"part_id": 1, "where": "id >= 50000 AND id < 100000"}]

            @task
            def prepare_target(task_cfg: dict) -> None:
                from ffengine.engine.target_writer import TargetWriter
                TargetWriter.prepare_table(task_cfg)
                # load_method kararları:
                # create_if_not_exists_or_truncate → CREATE + TRUNCATE
                # append                           → kontrol yok
                # replace                          → DROP + CREATE
                # upsert                           → var mı kontrol + CREATE
                # delete_from_table               → WHERE DELETE
                # drop_if_exists_and_create        → DROP IF EXISTS + CREATE

            @task
            def run_partition(partition_spec: dict, task_cfg: dict) -> dict:
                from ffengine.core.engine_interface import BaseEngine
                engine = BaseEngine.detect(task_cfg.get("engine", "auto"))
                result = engine.run_etl_task(task_cfg, partition_spec)
                return {
                    "part_id": partition_spec["part_id"],
                    "rows": result.rows,
                    "duration_seconds": result.duration_seconds,
                }

            specs = plan_partitions(etl_task)
            prep  = prepare_target(etl_task)
            specs >> prep >> run_partition.expand(partition_spec=specs)
```

## FFEngineOperator
```python
from ffengine.airflow.operator import FFEngineOperator

task = FFEngineOperator(
    task_id="run_orders",
    config_path="projects/webhook/whk/level1/src_to_stg/config.yaml",
    source_conn_id="src_oracle",
    target_conn_id="tgt_postgres",
    task_group_id="ocn_iss_orders_to_dwh_stg_orders_v1",
    engine="auto",      # auto | community | enterprise
)
```

## Operator Kuralı
`FFEngineOperator` yalnızca `BaseEngine` kontratını bilir.  
Engine detect/fallback mekanizması operatör seviyesinde şeffaf çalışmalıdır.  
`engine="auto"` → C Engine yüklüyse Enterprise, yoksa Community seçilir.

## XCom Anahtarları
Her `run_partition` tamamlandığında şu anahtarlar yazılır:
- `rows_transferred` → int
- `duration_seconds` → float
- `rows_per_second`  → float

## Level1 → Level2 Kademeli Tetikleme
```python
# Level1 DAG sonuna ekle
trigger_level2 = TriggerDagRunOperator(
    task_id="trigger_level2_stg_to_whk",
    trigger_dag_id="whk_level2_stg_to_whk",
    wait_for_completion=True,
    poke_interval=30,
)

# Tüm Level1 task grupları tamamlandıktan sonra tetikle
[tg_orders, tg_payments, tg_members] >> trigger_level2
```

## Binding Resolver — from Kaynakları
```yaml
bindings:
  last_updated:
    from: target        # source | target | literal | airflow_var
    sql: "SELECT MAX(updated_at) FROM dwh_stg.orders"
    default: "1900-01-01 00:00:00"
```

| from | Nereden okur | Tipik kullanım |
|---|---|---|
| `source` | Kaynak DB'ye sorgu çalıştırır | Kaynak tarafı watermark |
| `target` | Hedef DB'ye sorgu çalıştırır | DWH son yükleme tarihi |
| `literal` | YAML'daki sabit değeri alır | Sabit tarih / parametre |
| `airflow_var` | Airflow Variables tablosundan çeker | Runtime kontrollü değer |

Çözümleme sonucu `task_config["_resolved_where"]` alanına yazılır ve `plan_partitions` fazına iletilir.

## Partition Spec XCom Formatı
```python
# plan_partitions'ın döndürdüğü liste — her eleman bir partition worker'a gider
[
    {"part_id": 0, "where": "id >= 0 AND id < 33333"},
    {"part_id": 1, "where": "id >= 33333 AND id < 66666"},
    {"part_id": 2, "where": "id >= 66666 AND id < 100000"},
]
# full_scan senaryosunda:
[{"part_id": 0, "where": None}]
```

## Agent Kontrol Listesi
1. `on_failure_callback` default_args'a bağlanmış mı?
2. `plan → prepare → run` sırası `>>` operatörü ile kurulmuş mu?
3. `run_partition.expand(partition_spec=specs)` doğru mu?
4. Level1 DAG'da `TriggerDagRunOperator` var mı?
5. `engine="auto"` dışında bir değer seçildiyse gerekçe açıklanmış mı?
6. XCom anahtarları `rows_transferred`, `duration_seconds`, `rows_per_second` standartına uygun mu?