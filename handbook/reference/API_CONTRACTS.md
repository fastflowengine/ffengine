# API Contracts

## BaseEngine
```python
from abc import ABC, abstractmethod
from dataclasses import dataclass

@dataclass
class ETLResult:
    rows: int
    duration_seconds: float
    throughput: float
    partitions_completed: int
    errors: list[str]

class BaseEngine(ABC):
    @abstractmethod
    def run(self, config_path: str, task_group_id: str) -> ETLResult: ...

    @abstractmethod
    def is_available(self) -> bool: ...

    @classmethod
    def detect(cls, preference: str = "auto") -> "BaseEngine": ...
```

## BaseDialect
```python
class BaseDialect(ABC):
    def connect(self, params: dict): ...
    def create_cursor(self, conn, server_side: bool = False): ...
    def get_table_schema(self, conn, schema: str, table: str) -> list[dict]: ...
    def generate_ddl(self, schema: str, table: str, columns: list[dict]) -> str: ...
    def generate_bulk_insert_query(self, schema: str, table: str, columns: list[str]) -> str: ...
    def get_pagination_query(self, base_sql: str, offset: int, limit: int) -> str: ...
    def quote_identifier(self, name: str) -> str: ...
    def list_schemas(self, conn) -> list[str]: ...
    def list_tables(self, conn, schema: str, search: str | None = None, limit: int = 50) -> list[str]: ...

    # Yalnızca PostgresDialect Enterprise bulk extract path için
    def generate_bulk_extract_query(self, inner_sql: str) -> str: ...
```

## Community Engine Contracts
- `SourceReader.read() -> Generator[list[tuple], None, None]`
- `Streamer.stream(source_iter) -> ETLResult | dict`
- `TargetWriter.prepare(task_config: dict) -> None`
- `TargetWriter.write_batch(rows: list[tuple], task_config: dict) -> int`
- `TargetWriter.rollback_batch(exc: Exception | None = None) -> None`
- `Transformer.apply(rows: list[tuple], columns: list[dict], rules: dict | None = None) -> list[tuple]`
- `ETLManager.run_etl_task(task_config: dict, partition_spec: dict | None = None) -> ETLResult`

## Enterprise Queue Contracts
- `FFEnvelope(header: dict, payload: object)`
- `IngressQueue.put(envelope: FFEnvelope) -> None`
- `IngressQueue.get(timeout: float | None = None) -> FFEnvelope`
- `EgressQueue.ack(message_id: str) -> None`
- `EgressQueue.nack(message_id: str, exc: Exception) -> None`
- `CheckpointStore.save(offset_key: str, offset_value: str | int, ack_state: dict) -> None`
- `CheckpointStore.load(offset_key: str) -> dict | None`
- `DeliveryManager.resolve_semantics(task_config: dict) -> str`

## Config Resolution Convention
- Binding çözümlemesi sonrası iç alan: `task_config["_resolved_where"]`
- Partition spec formatı: `{"part_id": int, "where": str | None}`
- XCom summary anahtarları:
  - `rows_transferred`
  - `duration_seconds`
  - `rows_per_second`

## Delivery Rule
- Semantik seçimi için `reference/DELIVERY_GUARANTEE_MATRIX.md` tek otoritedir.
