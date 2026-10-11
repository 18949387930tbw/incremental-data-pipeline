# Incremental Data Pipeline

A Docker-free, incremental upgrade of the original
[beginner_de_project](https://github.com/josephmachado/beginner_de_project)
batch pipeline. The original Airflow + Spark + DuckDB + MinIO stack is kept
intact; a new `pipeline/` package turns the local batch script into a
checkpointed, idempotent, quality-aware pipeline with a FastAPI service.

* [What it does](#what-it-does)
* [Quick start (no Docker)](#quick-start-no-docker)
* [How the incremental pipeline works](#how-the-incremental-pipeline-works)
* [Data model](#data-model)
* [Data quality rules](#data-quality-rules)
* [HTTP API](#http-api)
* [Original Airflow stack](#original-airflow-stack)

## What it does

The pipeline reads two CSV sources, classifies movie reviews, joins them with
user purchases per customer, and produces behaviour metrics plus a Plotly
dashboard.

```text
CSV source
   |
   v
batch read (checkpoint + batch_size)
   |
   v
staging rows
   |
   v
data quality checks ---------> quarantine_record
   |
   v
raw tables (upsert by source_row_id)
   |
   v
customer_behaviour_metrics
   |
   +--> data/behaviour_metrics.csv
   +--> dags/scripts/dashboard/dashboard.html
   +--> FastAPI query endpoints
```

On the shipped sample data one full run reads **581,908** rows, lands
**437,923** clean rows, quarantines **143,985** bad rows and serves **889**
customer metrics.

## Quick start (no Docker)

Requirements: Python 3.11+ and Windows PowerShell.

```powershell
# 1. create .venv and install local dependencies
.\setup-local.ps1

# 2. run the pipeline (writes metrics CSV + dashboard, then opens it)
.\run-local.ps1

# 3. run the test suite
.\.venv\Scripts\python.exe -m pytest tests -q

# 4. start the API
.\.venv\Scripts\python.exe -m uvicorn pipeline.service:app --reload --port 8001
```

The API is then available at <http://127.0.0.1:8001/docs>.

The original command still works:

```powershell
.\.venv\Scripts\python.exe pipeline\run_local.py
```

### Replay from scratch

The warehouse lives in `data/warehouse.duckdb`. Delete that file to reprocess
every row from offset 0, then run the pipeline again.

## How the incremental pipeline works

1. Every run inserts a `pipeline_run` row with `status = running`.
2. Each source stores its progress in `source_checkpoint` as a **row offset**
   plus a **file fingerprint** (size + mtime).
3. `read_batch` reads at most `batch_size` rows (default 5000) after that
   offset and stamps each row with a stable `source_row_id`
   (`sha256(source_name, row_offset, raw_values)`).
4. Each batch runs inside one transaction:
   * bad rows go to `quarantine_record`
   * clean rows are staged and upserted with
     `insert ... on conflict (source_row_id) do nothing`
   * the checkpoint advances **only if the commit succeeds**
5. After all sources are processed the metrics table is rebuilt from the raw
   tables, the CSV is written and the dashboard is re-rendered.
6. Any failure rolls the batch back, keeps the old checkpoint and marks the run
   `failed` with the error message. Re-running resumes from the last safe
   checkpoint.

Because rows are keyed by `source_row_id` and metrics are recomputed, replaying
the same batch never duplicates rows or metrics.

### Idempotency and recovery at a glance

| Question | Answer |
| --- | --- |
| Will data be duplicated? | No - natural key upsert + full metric recompute |
| Where does a failed run resume? | From the last committed checkpoint |
| Where does dirty data go? | `quarantine_record`, queryable via the API |
| Can I safely re-run? | Yes, re-running is a no-op for already-landed rows |

## Data model

| Table | Purpose |
| --- | --- |
| `pipeline_run` | run history: status, counters, error message |
| `source_checkpoint` | per-source row offset + file fingerprint |
| `raw_movie_review` | landed reviews with `positive_review` flag |
| `raw_user_purchase` | landed purchases |
| `quarantine_record` | rejected rows with `reason_code` + raw payload |
| `customer_behaviour_metrics` | serving table, one row per customer |

## Data quality rules

A missing required **column** fails the whole source. Per-row problems are
quarantined with a stable `reason_code`:

| reason_code | meaning |
| --- | --- |
| `missing_required_field` | required value is blank (e.g. blank `CustomerID`) |
| `invalid_integer` | `cid` / `customer_id` is not an integer |
| `invalid_amount` | `unit_price` is not a number |
| `negative_quantity` | `quantity < 0` |
| `negative_unit_price` | `unit_price < 0` |
| `invalid_date` | `invoice_date` cannot be parsed |
| `duplicate_source_row` | same `source_row_id` seen twice in a batch |

Compared to the original script, rows that used to be silently dropped (blank
`CustomerID`) or silently aggregated (negative quantities) are now explicit:
they are written to `quarantine_record` and excluded from the metrics table.

## HTTP API

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/pipeline/runs` | trigger a run (`202`), `409 PIPELINE_ALREADY_RUNNING` if busy |
| `GET` | `/pipeline/runs` | paginated run history |
| `GET` | `/pipeline/runs/{run_id}` | single run status + counters |
| `GET` | `/pipeline/checkpoints` | per-source progress |
| `GET` | `/pipeline/quarantine` | paginated quarantine records |
| `GET` | `/metrics/behaviour` | paginated behaviour metrics |
| `GET` | `/health` | service + warehouse + last run status |

Only one run may be active at a time; the guard is a process-level
`threading.Lock` and runs execute as FastAPI `BackgroundTasks`.

## Project layout

```text
pipeline/
  config.py       paths + settings
  db.py           DuckDB connection + schema bootstrap
  schema.py       idempotent DDL
  sources.py      checkpointed batch CSV reader
  quality.py      quality rules + quarantine decisions
  transforms.py   review classification, metrics, dashboard
  runner.py       run orchestration and transaction boundaries
  service.py      FastAPI trigger + query service
  run_local.py    compatibility entry point
tests/
  pipeline/       unit + integration + acceptance tests
  fixtures/       tiny CSVs (never the 50MB samples)
```

## Configuration

`PipelineSettings` defaults to the repository layout, so no environment
variables are required. Tests build their own `PipelineSettings` pointing at
`tests/fixtures/` and a throwaway warehouse.

Key defaults:

* `batch_size = 5000`
* `pipeline_name = user_analytics_local`
* warehouse: `data/warehouse.duckdb`

## Original Airflow stack

The original Docker/Airflow/Spark instructions are preserved below. They are
not needed for the local incremental pipeline.
## Run Data Pipeline

Code available at **[beginner_de_project](https://github.com/josephmachado/beginner_de_project)** repository.

### Run on codespaces

You can run this data pipeline using GitHub codespaces. Follow the instructions below.

1. Create codespaces by going to the **[beginner_de_project](https://github.com/josephmachado/beginner_de_project)** repository, cloning it(or click `Use this template` button) and then clicking on `Create codespaces on main` button.
2. Wait for codespaces to start, then in the terminal type `make up`.
3. Wait for `make up` to complete, and then wait for 30s (for Airflow to start).
4. After 30s go to the `ports` tab and click on the link exposing port `8080` to access Airflow UI (username and password is `airflow`).

![Codespace](assets/images/cs1.png)
![Codespace make up](assets/images/cs2.png)
![Codespace Airflow UI](assets/images/cs3.png)

**Note** Make sure to switch off codespaces instance, you only have limited free usage; see docs [here](https://github.com/features/codespaces#pricing).

### Run locally

To run locally, you need:

1. [git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
2. [Github account](https://github.com/)
3. [Docker](https://docs.docker.com/engine/install/) with at least 4GB of RAM and [Docker Compose](https://docs.docker.com/compose/install/) v1.27.0 or later

Clone the repo and run the following commands to start the data pipeline:

```bash
git clone https://github.com/josephmachado/beginner_de_project.git
cd beginner_de_project 
make up
sleep 30 # wait for Airflow to start
make ci # run checks and tests
```

Go to [http:localhost:8080](http:localhost:8080) to see the Airflow UI. Username and password are both `airflow`.

## Architecture

This data engineering project, includes the following:

1. **`Airflow`**: To schedule and orchestrate DAGs.
2. **`Postgres`**: To store Airflow's details (which you can see via Airflow UI) and also has a schema to represent upstream databases.
3. **`DuckDB`**: To act as our warehouse
4. **`Quarto with Plotly`**: To convert code in `markdown` format to html files that can be embedded in your app or servered as is.
5. **`Apache Spark`**: To process our data, specifically to run a classification algorithm.
6. **`minio`**: To provide an S3 compatible open source storage system.

For simplicity services 1-5 of the above are installed and run in one container defined [here](./containers/airflow/Dockerfile).

![Data pipeline design](assets/images/arch.png)

The `user_analytics_dag` DAG in the [Airflow UI](http://localhost:8080) will look like the below image:

![DAG](assets/images/dag.png)

On completion, you can see the dashboard html rendered at[./dags/scripts/dashboard/dashboard.html](./dags/scripts/dashboard/dashboard.html).

Read **[this post](https://www.startdataengineering.com/post/data-engineering-projects-with-free-template/)**, for information on setting up CI/CD, IAC(terraform), "make" commands and automated testing.

