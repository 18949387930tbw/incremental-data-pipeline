# 数据管道增量处理平台：现有仓库原地改造设计

- 日期：2026-10-09
- 状态：待用户审查
- 改造仓库：`C:\Users\OMEN\Documents\Codex\2026-10-08\python-python-campus-space-booking-https\outputs\job-market-data-platform`
- 改造方式：在该仓库原地升级，不新建项目，保留原 Airflow、Spark、DuckDB、Quarto 资产
- 目标岗位：后端开发
- 时间预算：一个月中的配套项目，占约 30%

## 1. 背景

当前仓库原本是一个 Airflow + Spark + DuckDB + MinIO 的批处理数据工程示例。由于本机 CPU 虚拟化关闭，Docker 不可用，已经新增了基于 Python、DuckDB 和 pandas 的 `pipeline/run_local.py`，可以在没有 Docker 的情况下生成本地行为指标和 Plotly 看板。

当前脚本每次读取完整 CSV，失败后无法知道已经处理到哪里，也没有运行历史和数据质量隔离。本设计不推翻原数据管道，而是在现有仓库中把它升级为可增量、可恢复、可观测、可服务化的处理平台。

项目业务保持原有数据语义：用户购买行为和电影评论分析。简历中不把现有 CSV 伪称为招聘数据；如果后续需要换成真实招聘数据，只需要替换 source adapter，不改变本设计的核心架构。

## 2. 目标与非目标

### 目标

1. 保留原 Airflow DAG 和 Spark 示例文件，保证原仓库结构仍然可读。
2. 将 `pipeline/run_local.py` 拆成可测试的模块，保持命令兼容。
3. 增加 `pipeline_run`、`source_checkpoint` 和 `quarantine_record` 表。
4. 支持按批次增量读取、断点续跑和失败重试。
5. 使用自然键和 staging 表保证重复运行不产生重复指标。
6. 增加数据质量规则：必填字段、重复行、非法数量、非法金额、日期解析。
7. 增加 FastAPI 服务接口，支持异步触发和查询运行历史。
8. 提供幂等、断点续跑、质量校验和 API 测试。
9. 保持本地无 Docker 可运行。

### 非目标

1. 不在本阶段引入 Kafka、Flink、ClickHouse 或 Kubernetes。
2. 不把后台任务替换成 Celery 或 Redis；本地阶段使用 FastAPI BackgroundTasks 和进程内互斥锁。
3. 不删除原 Airflow DAG。
4. 不依赖云端对象存储；本地使用文件系统作为 source。
5. 不为了让数据看起来像招聘数据而伪造业务语义。

## 3. 目标架构

数据流：

```text
CSV source
   |
   v
批次读取（基于 checkpoint + batch_size）
   |
   v
staging 表
   |
   v
数据质量检查
   |            \
   |             v
   |        quarantine_record
   v
raw 表（按自然键 upsert）
   |
   v
customer_behaviour_metrics
   |
   +--> CSV 输出
   +--> dashboard.html
   +--> FastAPI 查询接口
```

核心模块：

- `pipeline/config.py`：路径、批次大小和数据库配置。
- `pipeline/db.py`：DuckDB 连接、建表和事务辅助函数。
- `pipeline/sources.py`：CSV source adapter、checkpoint 读取和批次切分。
- `pipeline/quality.py`：质量规则和 quarantine 写入。
- `pipeline/transforms.py`：行为指标计算和仪表盘渲染。
- `pipeline/runner.py`：一次运行的编排、状态更新和提交边界。
- `pipeline/service.py`：FastAPI 应用、后台任务和查询接口。
- `pipeline/run_local.py`：兼容入口，调用新 runner。

## 4. 数据模型

### pipeline_run

记录每次运行：

- `run_id: UUID`
- `pipeline_name: str`
- `status: running | succeeded | failed`
- `started_at`
- `finished_at`
- `rows_read`
- `rows_written`
- `rows_quarantined`
- `error_message`
- `created_at`

### source_checkpoint

记录每个 source 的处理进度：

- `source_name: str` 主键
- `cursor_value: str`
- `source_fingerprint: str`
- `updated_at`

本地 CSV 使用“已处理行偏移量 + 文件指纹”作为 cursor。文件指纹由文件大小和最后修改时间计算；如果文件被替换但指纹变化，运行不会静默跳数据。

### raw_movie_review

- `source_row_id: str` 主键，由 source 名称、行偏移和原始行内容生成哈希。
- `cid: int`
- `review_str: str`
- `source_name: str`
- `ingested_at`

### raw_user_purchase

- `source_row_id: str` 主键。
- `invoice_number`
- `stock_code`
- `quantity`
- `invoice_date`
- `unit_price`
- `customer_id`
- `country`
- `source_name`
- `ingested_at`

### quarantine_record

- `id: UUID`
- `run_id: UUID`
- `source_name: str`
- `source_row_id: str | None`
- `reason_code: str`
- `raw_payload: str`
- `created_at`

### customer_behaviour_metrics

- `customer_id: int`
- `amount_spent: float`
- `num_positive_reviews: int`
- `num_reviews: int`
- `updated_at`

## 5. 增量与幂等规则

1. 每次运行先创建 `pipeline_run(status=running)`。
2. 读取 checkpoint；没有 checkpoint 时从第 0 行开始。
3. 只读取 `batch_size` 行，默认 5000。
4. 写入 staging 表，不直接写最终表。
5. 执行质量规则，将不合格行写入 quarantine。
6. 合格行使用 `source_row_id` 做 upsert；重复运行同一批数据不会重复插入。
7. 所有批次成功后重新计算完整指标表。
8. 只有最终事务提交成功，才更新 checkpoint 和 `pipeline_run(status=succeeded)`。
9. 任一步失败则回滚当前批次，保留原 checkpoint，并记录 `failed` 和错误信息。
10. 再次运行从失败批次之前的安全 checkpoint 继续。

这个流程可以明确回答面试中的三个问题：数据会不会重复、失败后从哪里继续、脏数据去了哪里。

## 6. 数据质量规则

- 必填列缺失：整个 source 直接失败，不继续产生错误指标。
- `customer_id` 为空：隔离。
- `quantity` 或 `unit_price` 为负数：隔离。
- 金额无法解析：隔离。
- 重复 `source_row_id`：保留第一次，后续写入 quarantine。
- `cid` 或 `customer_id` 无法转换为整数：隔离。
- 日期格式无法解析：隔离并记录原因。
- 每次运行生成质量摘要：读取行数、合格行数、隔离行数和各 reason_code 数量。

质量规则应可单独测试，不依赖真实大文件。

## 7. API 设计

- `POST /pipeline/runs`：触发一次运行，返回 `202` 和 `run_id`。
- `GET /pipeline/runs`：分页查询运行历史。
- `GET /pipeline/runs/{run_id}`：查询单次状态和统计。
- `GET /pipeline/checkpoints`：查询 source 处理进度。
- `GET /pipeline/quarantine`：分页查询隔离记录。
- `GET /metrics/behaviour`：查询最新行为指标。
- `GET /health`：返回服务、DuckDB 和最后成功运行状态。

并发规则：同一时间只允许一个 pipeline 运行。已有运行中任务时，新的触发请求返回 `409 PIPELINE_ALREADY_RUNNING`。

## 8. 错误处理与事务边界

- 批次读取失败：标记 run failed，不推进 checkpoint。
- 质量规则失败：该行进入 quarantine，不阻塞其他合格行。
- upsert 失败：回滚当前批次并保留错误信息。
- 指标计算失败：不修改旧指标表，保留最后一次成功结果。
- 看板渲染失败：run 标记 failed，但原始 raw 表可以保留已提交批次，checkpoint 按事务边界推进。
- API 查询只读，不依赖运行中的内存状态。

## 9. 测试设计

必须增加：

- `test_incremental_rows_are_processed_once`：第一次运行只处理前 N 行，第二次处理剩余行。
- `test_same_batch_is_idempotent`：相同批次运行两次，raw 和 metrics 不重复。
- `test_checkpoint_not_advanced_on_failure`：注入失败后 checkpoint 保持原值。
- `test_negative_amount_goes_to_quarantine`：非法金额不进入指标表。
- `test_duplicate_source_row_is_quarantined`：重复行被记录。
- `test_dashboard_is_generated_after_success`：成功运行后生成 HTML。
- `test_pipeline_run_history_api`：API 能看到运行状态。
- `test_concurrent_trigger_is_rejected`：并发触发只允许一个运行。

测试使用小 CSV fixture，不复用 4 万行真实文件，保证快速稳定。

## 10. 需要修改的主要位置

保留：

- `dags/user_analytics.py`
- `dags/scripts/spark/random_text_classification.py`
- `dags/scripts/dashboard/dashboard.qmd`
- Airflow 和 Docker 相关配置文件

新增或调整：

- `pipeline/config.py`
- `pipeline/db.py`
- `pipeline/sources.py`
- `pipeline/quality.py`
- `pipeline/transforms.py`
- `pipeline/runner.py`
- `pipeline/service.py`
- `pipeline/run_local.py`
- `requirements-local.txt`
- `tests/pipeline/`
- `README.md`
- `.github/workflows/ci.yml`

## 11. 验收标准

1. `python pipeline/run_local.py` 仍然可以运行。
2. 连续运行两次不会重复指标。
3. 中途失败后可以基于 checkpoint 继续。
4. 劣质数据能在 quarantine 中查询。
5. `pipeline/run_local.py` 可以生成 `data/behaviour_metrics.csv` 和仪表盘。
6. FastAPI 可以触发运行并查询历史。
7. 本地不安装 Docker 也能完成全部测试。
8. README 能解释增量、幂等、质量和恢复策略。

## 12. 里程碑

- 第 1 周：读懂原脚本，拆分模块，建立 DuckDB schema。
- 第 2 周：批次读取、checkpoint 和幂等 upsert。
- 第 3 周：质量隔离、失败恢复和测试。
- 第 4 周：FastAPI 服务化、README、CI 和演示。

## 13. 简历可讲内容

- 将单体批处理脚本升级为有运行状态和 checkpoint 的增量数据管道。
- 使用自然键和 staging/upsert 实现幂等，解决重复运行的问题。
- 设计失败恢复边界，保证 checkpoint 只在事务成功后推进。
- 实现数据质量规则和 quarantine，避免脏数据污染指标。
- 使用 FastAPI 提供运行触发、历史查询和质量报告接口。