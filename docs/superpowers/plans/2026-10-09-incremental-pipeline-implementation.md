# 数据管道增量处理平台 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在现有 Airflow/Spark/DuckDB 仓库内，把本地全量脚本升级为支持 checkpoint、幂等、质量隔离、失败续跑和 FastAPI 查询的增量数据平台。

**Architecture:** 保留原 DAG、Spark 和 Quarto 文件；新增 `pipeline` 包，以 DuckDB 作为本地 warehouse。每次运行先写 `pipeline_run`，按 `source_checkpoint` 读取批次，写入 staging 表，执行质量规则，将不合格数据写入 quarantine，再用 `source_row_id` upsert 到 raw 表，最后重算指标和仪表盘。FastAPI 只负责触发和查询，不承载指标计算逻辑。

**Tech Stack:** Python 3.13, pandas, DuckDB, Plotly, FastAPI, pytest, DuckDB SQL。

**Spec:** `docs/superpowers/specs/2026-10-09-incremental-pipeline-design.md`

## Global Constraints

- 不新建仓库，不删除现有 Airflow DAG 和 Spark 示例。
- 本地必须无 Docker 运行。
- 优先使用增量读取；不能静默跳过未处理数据。
- `source_checkpoint` 只能在事务成功后推进。
- 重复运行同一批数据不得重复产生 raw 行或重复指标。
- 质量问题必须进入 quarantine，否则不能继续进入指标表。
- 每个任务先写失败测试，再写最小实现，再运行测试。
- 真实数据文件只用于最终演示，单元测试使用小 fixture。

---

### Task 1: 包结构、配置和 DuckDB schema

**Files:**
- Create: `pipeline/__init__.py`
- Create: `pipeline/config.py`
- Create: `pipeline/db.py`
- Create: `pipeline/schema.py`
- Create: `tests/pipeline/__init__.py`
- Create: `tests/pipeline/test_schema.py`
- Modify: `requirements-local.txt`

**Interfaces:**
- Produces: `PipelinePaths`, `PipelineSettings`
- Produces: `connect_warehouse(path) -> duckdb.DuckDBPyConnection`
- Produces: `initialize_schema(connection) -> None`

- [ ] **Step 0: Install local dependencies**

Run:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-local.txt
```

- [ ] **Step 1: Write failing schema tests**

```python
import duckdb

from pipeline.db import initialize_schema


def test_schema_contains_pipeline_tables(tmp_path):
    connection = duckdb.connect(str(tmp_path / "warehouse.duckdb"))
    initialize_schema(connection)
    names = {
        row[0]
        for row in connection.execute(
            "select table_name from information_schema.tables"
        ).fetchall()
    }
    assert {
        "pipeline_run",
        "source_checkpoint",
        "raw_movie_review",
        "raw_user_purchase",
        "quarantine_record",
        "customer_behaviour_metrics",
    }.issubset(names)
```

- [ ] **Step 2: Run and verify failure**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/pipeline/test_schema.py -q
```

Expected: import failure because `pipeline.db` and `initialize_schema` do not exist.

- [ ] **Step 3: Implement schema**

In `pipeline/schema.py`, define idempotent `CREATE TABLE IF NOT EXISTS` statements. Use `PRIMARY KEY` for `source_row_id`, `customer_id`, and `source_name` where appropriate. The metrics table must have a unique `customer_id`.

Add the needed local dependencies:

```text
pandas>=2.2
duckdb>=1.1.0
plotly>=5.24
pyarrow>=16.0
fastapi>=0.115
uvicorn[standard]>=0.30
httpx>=0.27
pytest>=8.0
```

- [ ] **Step 4: Run schema tests**

Run: `.\.venv\Scripts\python.exe -m pytest tests/pipeline/test_schema.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add pipeline requirements-local.txt tests/pipeline
git commit -m "feat: add incremental pipeline schema"
```

### Task 2: Checkpoint 和按批次读取

**Files:**
- Create: `pipeline/sources.py`
- Create: `tests/pipeline/test_sources.py`
- Create: `tests/fixtures/__init__.py`
- Create: `tests/fixtures/tiny_movie_review.csv`
- Create: `tests/fixtures/tiny_user_purchase.csv`

**Interfaces:**
- Produces: `SourceBatch`
- Produces: `read_checkpoint(connection, source_name) -> int`
- Produces: `read_batch(source_name, offset, batch_size) -> SourceBatch`
- Produces: `update_checkpoint(connection, source_name, new_offset, fingerprint) -> None`

- [ ] **Step 1: Write failing checkpoint tests**

```python
from pipeline.sources import read_batch, read_checkpoint


def test_read_batch_returns_next_five_rows(tmp_path):
    source = tmp_path / "reviews.csv"
    source.write_text("cid,review_str\n1,good\n2,bad\n3,good\n", encoding="utf-8")
    batch = read_batch(source, offset=1, batch_size=1)
    assert len(batch.rows) == 1
    assert batch.next_offset == 2
    assert batch.rows[0]["review_str"] == "bad"


def test_missing_checkpoint_starts_at_zero(connection):
    assert read_checkpoint(connection, "movie_review") == 0
```

- [ ] **Step 2: Run and verify failure**

Run: `.\.venv\Scripts\python.exe -m pytest tests/pipeline/test_sources.py -q`

Expected: import failure because `pipeline.sources` is missing.

- [ ] **Step 3: Implement source reader**

`read_batch` must:

1. Read only the requested CSV slice.
2. Preserve source row number.
3. Compute `source_row_id` from `source_name`, row number, and the raw row content.
4. Return `next_offset = offset + len(rows)`.
5. Never return rows outside the requested batch.

Use a dataclass:

```python
@dataclass(frozen=True)
class SourceBatch:
    source_name: str
    rows: list[dict]
    next_offset: int
    fingerprint: str
```

- [ ] **Step 4: Run source tests**

Run: `.\.venv\Scripts\python.exe -m pytest tests/pipeline/test_sources.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add pipeline/sources.py tests/pipeline/test_sources.py tests/fixtures
git commit -m "feat: add checkpointed csv source reader"
```

### Task 3: 质量规则与 quarantine

**Files:**
- Create: `pipeline/quality.py`
- Create: `tests/pipeline/test_quality.py`

**Interfaces:**
- Produces: `QualityResult(clean_rows, quarantined_rows)`
- Produces: `validate_movie_review(rows) -> QualityResult`
- Produces: `validate_user_purchase(rows) -> QualityResult`

- [ ] **Step 1: Write failing quality tests**

```python
def test_negative_quantity_is_quarantined():
    result = validate_user_purchase([
        {
            "source_row_id": "row-1",
            "customer_id": 1,
            "quantity": -2,
            "unit_price": 10.0,
            "invoice_date": "12/1/2010 8:26",
        }
    ])
    assert result.clean_rows == []
    assert result.quarantined_rows[0]["reason_code"] == "negative_quantity"


def test_valid_purchase_is_accepted():
    result = validate_user_purchase([
        {
            "source_row_id": "row-2",
            "customer_id": 1,
            "quantity": 2,
            "unit_price": 10.0,
            "invoice_date": "12/1/2010 8:26",
        }
    ])
    assert len(result.clean_rows) == 1
```

- [ ] **Step 2: Run and verify failure**

Run: `.\.venv\Scripts\python.exe -m pytest tests/pipeline/test_quality.py -q`

Expected: import failure.

- [ ] **Step 3: Implement quality rules**

Use reason codes that can be tested independently:

- `missing_required_field`
- `invalid_integer`
- `negative_quantity`
- `negative_unit_price`
- `invalid_date`
- `duplicate_source_row`

Do not silently drop bad rows. Preserve `raw_payload` so the row can be inspected later.

- [ ] **Step 4: Run quality tests**

Run: `.\.venv\Scripts\python.exe -m pytest tests/pipeline/test_quality.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add pipeline/quality.py tests/pipeline/test_quality.py
git commit -m "feat: add data quality quarantine rules"
```

### Task 4: Staging、幂等 upsert 和运行编排

**Files:**
- Create: `pipeline/transforms.py`
- Create: `pipeline/runner.py`
- Modify: `pipeline/run_local.py`
- Create: `tests/pipeline/test_runner.py`

**Interfaces:**
- Produces: `run_pipeline(settings, batch_size=5000) -> str`
- Produces: `upsert_raw_rows(connection, source_name, rows) -> int`
- Produces: `build_metrics(connection) -> None`
- Produces: `render_dashboard(metrics) -> None`

- [ ] **Step 1: Write failing idempotency test**

```python
def test_running_same_batch_twice_does_not_duplicate_metrics(pipeline_settings):
    first_run = run_pipeline(pipeline_settings, batch_size=2)
    second_run = run_pipeline(pipeline_settings, batch_size=2)
    connection = connect_warehouse(pipeline_settings.warehouse_path)
    raw_count = connection.execute("select count(*) from raw_movie_review").fetchone()[0]
    metric_count = connection.execute("select count(*) from customer_behaviour_metrics").fetchone()[0]
    assert first_run != second_run
    assert raw_count == 4
    assert metric_count > 0
```

- [ ] **Step 2: Run and verify failure**

Run: `.\.venv\Scripts\python.exe -m pytest tests/pipeline/test_runner.py -q`

Expected: import or function failure because `run_pipeline` does not exist.

- [ ] **Step 3: Implement transaction boundaries**

For each source:

1. Read one batch.
2. Insert into a temporary staging table with `source_row_id`.
3. Run quality checks.
4. Insert quarantine records.
5. Upsert clean rows with `INSERT ... ON CONFLICT (source_row_id) DO NOTHING`.
6. Update checkpoint only after the transaction succeeds.
7. On exception, roll back the batch and mark `pipeline_run` failed.

Make `pipeline/run_local.py` a compatibility wrapper:

```python
from pipeline.runner import run_pipeline

if __name__ == "__main__":
    run_pipeline(PipelineSettings())
```

- [ ] **Step 4: Run the runner and existing pipeline command**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/pipeline/test_runner.py -q
.\.venv\Scripts\python.exe pipeline\run_local.py
```

Expected: tests pass and the command still writes metrics and dashboard.

- [ ] **Step 5: Commit**

```powershell
git add pipeline tests/pipeline
git commit -m "feat: add idempotent incremental pipeline runner"
```

### Task 5: FastAPI 触发和查询接口

**Files:**
- Create: `pipeline/service.py`
- Create: `tests/pipeline/test_service.py`
- Modify: `requirements-local.txt`

**Interfaces:**
- Produces: `PipelineRunResponse`
- Produces: `POST /pipeline/runs`
- Produces: `GET /pipeline/runs`
- Produces: `GET /pipeline/runs/{run_id}`
- Produces: `GET /pipeline/checkpoints`
- Produces: `GET /pipeline/quarantine`
- Produces: `GET /metrics/behaviour`

- [ ] **Step 1: Write failing API tests**

```python
from fastapi.testclient import TestClient
from pipeline.service import app


def test_health_endpoint():
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_trigger_run_returns_202():
    client = TestClient(app)
    response = client.post("/pipeline/runs", json={"batch_size": 10})
    assert response.status_code == 202
    assert "run_id" in response.json()
```

- [ ] **Step 2: Run and verify failure**

Run: `.\.venv\Scripts\python.exe -m pytest tests/pipeline/test_service.py -q`

Expected: import failure because `pipeline.service` is missing.

- [ ] **Step 3: Implement API and process lock**

Use `BackgroundTasks` to call `run_pipeline`. Use a process-level `threading.Lock` or atomic running check to return `409 PIPELINE_ALREADY_RUNNING` when a run is already active. Query endpoints read only from DuckDB and return JSON-safe values.

- [ ] **Step 4: Run API tests**

Run: `.\.venv\Scripts\python.exe -m pytest tests/pipeline/test_service.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add pipeline/service.py tests/pipeline/test_service.py requirements-local.txt
git commit -m "feat: expose pipeline run and query api"
```

### Task 6: README、CI 和最终验收

**Files:**
- Modify: `README.md`
- Modify: `.github/workflows/ci.yml`
- Modify: `run-local.ps1`
- Modify: `setup-local.ps1`
- Modify: `.gitignore`

- [ ] **Step 1: Add a failing acceptance test**

Add a test that runs a small fixture through the pipeline twice and asserts:

1. checkpoint advances only after success；
2. second run has no duplicate `source_row_id`；
3. quarantine contains at least one invalid row from the fixture；
4. metrics table contains only clean data.

- [ ] **Step 2: Run and verify failure**

Run: `.\.venv\Scripts\python.exe -m pytest tests/pipeline/test_acceptance.py -q`

Expected: FAIL until all acceptance behavior is connected.

- [ ] **Step 3: Document and wire local commands**

`README.md` must include:

```powershell
.\.\setup-local.ps1
.\.\run-local.ps1
.\.venv\Scripts\python.exe -m uvicorn pipeline.service:app --reload --port 8001
.\.venv\Scripts\python.exe -m pytest tests -q
```

Document checkpoint, idempotency, quarantine, API endpoints, and the difference from the original Airflow DAG.

- [ ] **Step 4: Run full acceptance**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests -q
.\.venv\Scripts\python.exe pipeline\run_local.py
```

Expected: all tests pass and the dashboard is regenerated.

- [ ] **Step 5: Commit**

```powershell
git add README.md .github/workflows/ci.yml run-local.ps1 setup-local.ps1 .gitignore tests
git commit -m "docs: validate incremental pipeline workflow"
```