# 🏗️ Databricks Commodity Prices Project — Development Checklist

> **Project:** Commodity Prices Dashboard with Economic Indicators  
> **Goal:** Learn Databricks, DAB, Spark, Delta Lake, and Data Warehousing fundamentals through a hands-on project  
> **Budget:** ~15 development hours  
> **Guiding Principle:** Build it like it's going to production, even if it's just for learning.

---

## How to Use This Checklist

Each task has two sub-sections:
- **✅ What to Do** — The concrete action items
- **📚 Research Scope** — Concepts, design decisions, and things to understand *before* or *while* doing the task

Track your progress by checking off boxes. Estimated hours are guidelines — go slower where you're learning the most.

---

## Phase 0 — Project Setup & Environment ⏱️ ~1.5 hrs

### 0.1 — Provision Your Databricks Environment

- [x] Sign up for **Databricks Community Edition** (free) or set up a trial workspace on AWS/Azure/GCP
- [x] Create a **cluster** (single-node is fine; use DBR 13.x or later for Unity Catalog support)
-     (Turns out free edtion only allows serverless compute)
- [x] Familiarise yourself with the Workspace UI: Notebooks, Repos, Compute, SQL Warehouse, Catalog Explorer

**📚 Research Scope:**
- Understand the difference between **All-Purpose Clusters** (interactive, always-on) vs **Job Clusters** (ephemeral, triggered by a job). For a learning project, you'll use all-purpose clusters, but know the distinction — in production, job clusters are preferred for cost control.
- Understand what a **SQL Warehouse** is and how it differs from a Spark cluster. SQL Warehouses are optimised for BI queries (your dashboard will use one). They are serverless-friendly and support ANSI SQL.
- Read about **Databricks Runtime (DBR)** versions and why pinning a version matters for reproducibility.

---

### 0.2 — Set Up Databricks Asset Bundles (DAB)

- [x] Install the **Databricks CLI** (`pip install databricks-cli` or the newer `databricks` CLI v0.200+)
- [x] Authenticate the CLI to your workspace (`databricks auth login`)
- [x] Initialise a DAB project: `databricks bundle init` using the default template
- [x] Understand the generated `databricks.yml` file structure
- [ ] Create `dev` and `prod` targets in your bundle config

**📚 Research Scope:**
- **What is DAB?** Databricks Asset Bundles is Databricks' native Infrastructure-as-Code (IaC) framework. It lets you define notebooks, jobs, pipelines, clusters, and permissions in YAML and deploy them programmatically — similar to how Terraform works for cloud infra.
- Understand the anatomy of `databricks.yml`: `bundle`, `workspace`, `resources` (jobs, pipelines, clusters), and `targets` (dev, staging, prod).
- Key concept: **Why IaC for a learning project?** Because deploying manually via the UI is not repeatable. DAB teaches you the habit of treating your data pipeline as code — version-controlled, peer-reviewable, and deployable with a single command.
- Look up the difference between **DAB** and the older **dbx** tool — DAB is the current recommended approach.

---

### 0.3 — Set Up Git Integration

- [x] Create a GitHub repository for the project
- [x] Connect your Databricks Workspace to GitHub via **Repos** (Workspace → Repos → Add Repo)
- [ ] Establish a folder structure (suggested below):

```
commodity-prices/
├── databricks.yml               # DAB bundle config
├── src/
│   ├── ingestion/               # Bronze layer notebooks/scripts
│   ├── transformation/          # Silver layer notebooks/scripts
│   ├── aggregation/             # Gold layer notebooks/scripts
│   └── utils/                   # Shared helpers
├── data/
│   └── raw/                     # Local CSV samples for testing
├── tests/                       # Unit tests
└── README.md
```

**📚 Research Scope:**
- Understand **Databricks Repos** — it's a Git-backed workspace folder. Changes you make in notebooks are reflected as file changes in Git.
- Know the difference between editing in Repos (version-controlled) vs the regular Workspace (not version-controlled by default). Always work in Repos.

---

## Phase 1 — Storage Architecture & Bronze Layer ⏱️ ~2.5 hrs

### 1.1 — Design Your Medallion Architecture

- [ ] Draw (on paper or in a tool like draw.io) your three-layer architecture:
  - **Bronze** — Raw, unmodified CSV data landed as-is
  - **Silver** — Cleaned, typed, deduplicated data
  - **Gold** — Aggregated, business-ready data for the dashboard
- [ ] Decide on your catalog/schema naming: e.g., `commodity_prices.bronze`, `commodity_prices.silver`, `commodity_prices.gold`

**📚 Research Scope:**
- **Medallion Architecture** — This is the industry standard pattern for organising data in a Lakehouse. Bronze = landing zone (raw truth), Silver = conformed layer (clean and typed), Gold = serving layer (aggregated for consumption). The key insight is that you *never overwrite Bronze* — it is an immutable record of what arrived.
- **Unity Catalog hierarchy**: `Catalog → Schema (Database) → Table/Volume`. Understand this three-level namespace and why it was introduced (to unify governance across workspaces).
- **Volumes vs Tables** — In Unity Catalog, a **Volume** is a governed storage path for files (like CSVs, JSONs), while a **Table** is a structured Delta table. Your raw CSVs will live in a Bronze **Volume**, and your processed data will live in **Delta Tables**. Know this distinction before you build.

---

### 1.2 — Create Unity Catalog Resources

- [ ] Create a Catalog: `commodity_prices` (via Catalog Explorer UI or SQL)
- [ ] Create Schemas: `bronze`, `silver`, `gold`
- [ ] Create a **Volume** in the bronze schema for raw file storage: `commodity_prices.bronze.raw_files`
- [ ] Upload your raw CSV files into this Volume

**📚 Research Scope:**
- Run these SQL commands in a notebook to understand Unity Catalog DDL:
  ```sql
  CREATE CATALOG IF NOT EXISTS commodity_prices;
  CREATE SCHEMA IF NOT EXISTS commodity_prices.bronze;
  CREATE VOLUME IF NOT EXISTS commodity_prices.bronze.raw_files;
  ```
- Understand **managed vs external tables** in Unity Catalog. Managed tables store their data inside Databricks-managed storage. External tables point to your own cloud storage path. For this project, use **managed tables** — simpler, and Unity Catalog handles lifecycle.
- Understand what **DBFS (Databricks File System)** is and why it is being **deprecated** in favour of Unity Catalog Volumes. Don't use `/dbfs/` paths — use Volume paths (`/Volumes/...`) instead.

---

### 1.3 — Build the Bronze Ingestion Pipeline

- [ ] Write a notebook/script `src/ingestion/ingest_bronze.py` that:
  - Reads CSV files from the Volume using Spark (`spark.read.csv(...)`)
  - Adds metadata columns: `_source_file`, `_ingestion_timestamp`, `_batch_id`
  - Writes to a **Delta table** in the bronze schema using **APPEND** mode
- [ ] Test with one commodity file first (e.g., Gold prices), then generalise for all files
- [ ] Register this notebook as a **DAB Job** in `databricks.yml`

**📚 Research Scope:**
- **Why Delta format for Bronze?** Delta Lake provides ACID transactions, schema enforcement, and time travel — even for your raw landing zone. The old approach was to store raw data as raw Parquet or CSV on a data lake. Delta is strictly better for the Lakehouse pattern.
- **Spark `read.csv` options** — Understand `header`, `inferSchema`, `schema` (providing an explicit schema), and `multiLine`. Know why `inferSchema=True` is convenient but risky in production (it requires a full scan and can be wrong).
- **Schema-on-read vs Schema-on-write** — CSVs are schema-on-read (you infer the schema when you read). Delta tables are schema-on-write (you define/enforce schema at write time). Bronze is a transitional zone — you're moving from one to the other.
- **Auto Loader** (`cloudFiles`) — Research this even if you don't use it immediately. Auto Loader is Databricks' scalable, incremental file ingestion mechanism that tracks which files have already been processed using checkpoints. For a production system with continuously arriving CSVs, this is the right tool. For this project, understand the concept and optionally implement it.
- **`_rescued_data` column** — When using `cloudFiles` or schema enforcement, Databricks can automatically capture malformed rows into a rescue column. Research this for resilient ingestion.

---

### 1.4 — Handle Late-Arriving Files (Production Problem #1)

- [ ] Simulate a late-arriving file: add a CSV for a past date and re-run your ingestion pipeline
- [ ] Verify that the Bronze table correctly appended the new rows without duplicating existing ones
- [ ] Add a deduplication step using `dropDuplicates()` or `MERGE` at the Silver layer

**📚 Research Scope:**
- **Late-arriving data** is one of the most common real-world data engineering problems. It occurs when source data arrives after the scheduled processing window — e.g., you've already processed "January data" and then a corrected January file arrives in February.
- The Bronze layer *should* accept late data unconditionally (append it). The Silver layer is where you reconcile and deduplicate.
- **Delta Lake `MERGE` (UPSERT)** — This is the key operation for handling late arrivals at Silver. Learn the `MERGE INTO` syntax:
  ```sql
  MERGE INTO silver.commodity_prices AS target
  USING new_data AS source
  ON target.date = source.date AND target.commodity = source.commodity
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
  ```
- **Delta Time Travel** — because Delta stores transaction history, you can query the state of a table *before* a late-arriving record was merged. This is `SELECT * FROM table VERSION AS OF 5` or `TIMESTAMP AS OF '2024-01-01'`. This is your "undo" mechanism — invaluable for debugging production incidents.

---

## Phase 2 — Silver Layer: Cleaning & Conforming ⏱️ ~2.5 hrs

### 2.1 — Build the Silver Transformation Notebook

- [ ] Write `src/transformation/transform_silver.py` that:
  - Reads from the Bronze Delta table
  - Casts columns to correct types (date → `DateType`, price → `DoubleType`, volume → `LongType`)
  - Standardises column names (snake_case, consistent naming)
  - Drops true duplicate rows
  - Handles nulls — decide: fill, drop, or flag?
  - Writes to `commodity_prices.silver.commodity_prices` using `MERGE` (upsert, not overwrite)

**📚 Research Scope:**
- **Spark DataTypes** — Know the difference between `StringType`, `IntegerType`, `LongType`, `DoubleType`, `DecimalType`, `DateType`, and `TimestampType`. For financial data, use `DecimalType(18, 6)` for prices, not `DoubleType` (floating-point imprecision matters in finance). For this learning project, `DoubleType` is fine, but know *why* production systems use Decimal.
- **Spark Transformations vs Actions** — Understand the lazy evaluation model. Transformations (`.filter()`, `.select()`, `.join()`) build a logical plan. Actions (`.write`, `.count()`, `.show()`) trigger execution. This is fundamental to how Spark achieves parallelism.
- **`withColumn` vs `select`** — Both can transform columns. `select` is generally preferred for performance because it's explicit about the output schema. `withColumn` adds/replaces one column at a time and can be less efficient when chaining many transformations.
- **Handling nulls**: `fillna()`, `dropna()`, `na.fill()`, and `when(col.isNull(), ...)`. Understand the difference between NULL in Spark vs empty string. For time series commodity data, a null price might mean the market was closed — decide your business rule.

---

### 2.2 — Add a Slowly Changing Dimension (SCD) for Commodities

- [ ] Create a `commodity_dim` table in Silver with commodity metadata (name, unit, category, exchange)
- [ ] Implement **SCD Type 1** (overwrite) first — simplest to understand
- [ ] Then refactor to **SCD Type 2** (keep history with `valid_from`, `valid_to`, `is_current` columns)

**📚 Research Scope:**
- **Slowly Changing Dimensions (SCDs)** are one of the most important concepts in data warehousing. A dimension (like a commodity's metadata) changes infrequently but meaningfully. The SCD type you choose determines how you handle history.
  - **Type 1** — Overwrite. No history. Simple. Use when history doesn't matter.
  - **Type 2** — Add a new row for each change. Full history. Complex. Use when you need point-in-time accuracy (e.g., "what was the exchange for Gold *in 2020*?"). Requires `valid_from`, `valid_to`, `is_current` columns.
  - **Type 3** — Add a new column for each change (e.g., `current_exchange`, `previous_exchange`). Limited history. Rare.
- For this project: implement Type 2 using Delta's `MERGE` operation. This is a rite of passage for data engineers — nearly every production warehouse has SCD Type 2 tables.
- **Surrogate keys vs natural keys** — In your dimension table, you should have a `commodity_sk` (surrogate key, auto-generated integer) separate from `commodity_id` (natural key, like "XAU" for Gold). Surrogate keys are required for SCD Type 2 because the same natural key can have multiple rows.

---

### 2.3 — Handle a New Commodity (Production Problem #2)

- [ ] Simulate adding a new commodity mid-project: create a new CSV for "Natural Gas" and run the ingestion
- [ ] Ensure your Silver pipeline handles it without code changes (schema evolution)
- [ ] Add the new commodity to your dimension table

**📚 Research Scope:**
- **Schema Evolution in Delta Lake** — Delta can automatically handle new columns being added to source data using `mergeSchema` option:
  ```python
  df.write.option("mergeSchema", "true").format("delta").mode("append").save(...)
  ```
  Understand when you'd enable this vs enforce a strict schema. In Bronze, enable it. In Gold, be more restrictive.
- **Schema Enforcement** — By default, Delta rejects writes that don't match the existing schema. This is a *feature*, not a bug. It prevents silent data corruption. Learn how to use `ALTER TABLE ... ADD COLUMN` for intentional schema changes.
- The key engineering lesson here is: **data pipelines must be designed to handle new entities without requiring code deployments**. If your pipeline breaks every time a new commodity is added, it's not production-ready.

---

## Phase 3 — Gold Layer: Aggregation & Data Modelling ⏱️ ~2.5 hrs

### 3.1 — Design the Gold Layer Data Model

- [ ] Design a **star schema** with:
  - Fact table: `fact_commodity_prices` (date_sk, commodity_sk, price, volume, price_change_pct)
  - Dimension: `dim_commodity` (SCD Type 2 from Phase 2)
  - Dimension: `dim_date` (a pre-built date dimension with year, quarter, month, week, day_of_week)
  - Fact table: `fact_economic_indicators` (date_sk, indicator_sk, value)
  - Dimension: `dim_economic_indicator` (indicator name, unit, source, frequency)

**📚 Research Scope:**
- **Star Schema vs Snowflake Schema** — Star schema is the standard for analytical workloads. Dimensions are denormalised (a single flat table), making queries faster and simpler. Snowflake schema further normalises dimensions into sub-dimensions. For a dashboard, **always use a star schema**.
- **Fact tables** contain measurable events (prices, trades) and foreign keys to dimensions. They are wide, long, and append-heavy.
- **Dimension tables** contain descriptive attributes. They are narrow and relatively small.
- **Date dimension** — Every data warehouse has one. It's a pre-generated table with one row per day and many derived columns (is_weekend, fiscal_quarter, etc.). Never derive these in your fact queries — pre-compute them in the dimension.
- **Grain** — The grain of your fact table is the level of detail. For `fact_commodity_prices`, the grain is one row per commodity per day. **Define the grain before you write a single line of code.** Changing the grain later is expensive.
- **Surrogate keys and joins** — Your fact table should join to dimensions using surrogate keys (integers), not natural keys (strings). Integer joins are significantly faster at scale.

---

### 3.2 — Build the Gold Aggregation Notebook

- [ ] Write `src/aggregation/build_gold.py` that:
  - Reads from Silver commodity prices and Silver economic indicators
  - Joins to the date dimension
  - Computes derived metrics: `daily_return`, `rolling_7d_avg`, `rolling_30d_avg`, `ytd_return`
  - Writes to Gold Delta tables (overwrite on full refresh, or incremental using `MERGE`)

**📚 Research Scope:**
- **Spark Window Functions** — Essential for time series calculations. Window functions compute values across a sliding window of rows without collapsing the result (unlike `GROUP BY`). Know these:
  ```python
  from pyspark.sql.window import Window
  from pyspark.sql import functions as F

  w = Window.partitionBy("commodity_id").orderBy("date")
  df = df.withColumn("daily_return", (F.col("price") - F.lag("price", 1).over(w)) / F.lag("price", 1).over(w))
  df = df.withColumn("rolling_30d_avg", F.avg("price").over(w.rowsBetween(-29, 0)))
  ```
  Understand `partitionBy`, `orderBy`, `rowsBetween`, and `rangeBetween`.
- **`PARTITION BY` in Delta tables** — Understand physical partitioning of Delta tables (partitioning files by a column like `year` or `commodity_id`) vs logical partitioning in Window functions. Physical partitioning helps query performance by allowing Spark to skip irrelevant files. For your gold layer, consider partitioning `fact_commodity_prices` by `commodity_id`.
- **`Z-ORDER` clustering** — Delta Lake's data skipping optimisation. If you frequently filter by `date`, running `OPTIMIZE table ZORDER BY (date)` co-locates related data, dramatically speeding up queries. Research how this differs from partitioning.

---

### 3.3 — Implement Incremental Processing

- [ ] Modify your Gold build to be incremental: only process new records since the last run
- [ ] Use a **watermark pattern**: store the `last_processed_timestamp` in a control table and use it to filter new Silver records
- [ ] Test by adding new Silver records and verifying Gold updates without re-processing everything

**📚 Research Scope:**
- **Full refresh vs incremental** — Full refresh re-processes everything every run. Simple but slow. Incremental only processes new/changed data. Faster but complex. For large datasets, incremental is mandatory.
- **Control tables / bookmarks** — A simple Delta table with one row per pipeline that stores the high-water mark (last processed timestamp or max surrogate key). Before each run, read the bookmark. After each run, update it. This is the simplest form of incremental state management.
- **Structured Streaming** — For truly real-time incremental processing, Spark has a Structured Streaming API. It uses Delta tables as streaming sources/sinks and manages checkpoints automatically. Research this even if you implement batch-incremental for now. In production, high-frequency pipelines use Structured Streaming.

---

## Phase 4 — Databricks SQL & Dashboard ⏱️ ~2 hrs

### 4.1 — Create a SQL Warehouse

- [ ] Create a **SQL Warehouse** (Serverless or Pro) in your workspace
- [ ] Connect it to your Unity Catalog
- [ ] Verify you can query your Gold tables in the **SQL Editor**

**📚 Research Scope:**
- SQL Warehouses run ANSI SQL and are optimised for BI/analytical queries — they use **Photon** (Databricks' vectorised query engine written in C++) under the hood, making them significantly faster than standard Spark for SQL workloads.
- **Serverless SQL Warehouse vs Classic** — Serverless starts in ~5 seconds (no cluster to provision). Classic takes 3-5 minutes to start. For development and dashboards, Serverless is strongly preferred.
- Understand **auto-stop** settings — SQL Warehouses auto-stop after inactivity to save cost.

---

### 4.2 — Write Dashboard Queries

- [ ] Write and save the following queries in the SQL Editor:
  - Commodity price over a selected time range (parameterised by commodity and date range)
  - Price change % (daily, monthly, YTD) for a selected commodity
  - Rolling average overlay (7d, 30d)
  - Economic indicator trend for the same time period
  - Correlation view: commodity price vs economic indicator on a dual-axis chart

**📚 Research Scope:**
- **Query Parameters in Databricks SQL** — Use `{{ parameter_name }}` syntax to make queries dynamic. This powers the interactive filters on your dashboard.
- **`PIVOT`** — Useful for turning row-based time series into columnar format for comparison charts.
- **`DATE_TRUNC` and `DATE_DIFF`** — Essential date manipulation functions for time series aggregation (e.g., `DATE_TRUNC('month', date)` to aggregate by month).
- Research **Databricks SQL Query History** — understand how to debug slow queries using the query profile (execution plan, stages, spill to disk).

---

### 4.3 — Build the Dashboard

- [ ] Create a new **Dashboard** in Databricks SQL
- [ ] Add widgets for:
  - A date range filter (global, applied to all charts)
  - A commodity selector (dropdown)
  - An economic indicator selector (dropdown)
  - A line chart: commodity price over time
  - A bar chart: monthly price change %
  - A line chart overlay: rolling averages
  - A line chart: economic indicator over the same period
- [ ] Set up **auto-refresh** on the dashboard (e.g., every 24 hours)

**📚 Research Scope:**
- **Lakeview Dashboards** (the newer AI/BI dashboards in Databricks) vs the older **DBSQL Dashboards** — Databricks is migrating to Lakeview. Use Lakeview if available in your workspace. It has a more modern editor and better parameter binding.
- Understand the relationship between **visualisations**, **queries**, and **dashboards** in Databricks SQL: a query produces results, a visualisation renders those results as a chart, a dashboard assembles multiple visualisations with shared parameters.

---

## Phase 5 — Orchestration with Databricks Jobs ⏱️ ~1.5 hrs

### 5.1 — Design the Job DAG

- [ ] Define the full pipeline as a multi-task **Databricks Job** with dependencies:
  ```
  ingest_bronze → transform_silver → build_gold → (optional) refresh_dashboard
  ```
- [ ] Add a `trigger` in `databricks.yml` (e.g., daily cron schedule)
- [ ] Deploy using `databricks bundle deploy --target dev`

**📚 Research Scope:**
- **Databricks Jobs** — A Job is a scheduled or triggered execution of one or more tasks. Tasks can be notebooks, Python files, SQL queries, or Delta Live Tables pipelines. Tasks can depend on each other, forming a DAG (Directed Acyclic Graph).
- **DAG (Directed Acyclic Graph)** — The concept of tasks with dependencies where no circular dependencies are allowed. It's the fundamental model for all workflow orchestrators (Airflow, Prefect, dbt, and Databricks Jobs all use DAGs).
- **`depends_on`** in DAB `databricks.yml` — This is how you express task dependencies in your bundle config.
- **Job cluster vs all-purpose cluster** — Define a `new_cluster` spec in your job config so it spins up a fresh cluster for each run (cost-efficient) instead of running on your interactive cluster.

---

### 5.2 — Add Error Handling and Notifications

- [ ] Add `try/except` blocks in your notebooks with meaningful error messages
- [ ] Configure **email notifications** on job failure in the DAB job config
- [ ] Add a simple data quality check: assert row count > 0 after each layer write

**📚 Research Scope:**
- **Idempotency** — A pipeline is idempotent if running it multiple times produces the same result as running it once. Your pipelines should be idempotent so you can safely re-run them after failures. This is achieved by using `MERGE` instead of `INSERT`, and by tracking processed files/timestamps.
- **Data quality frameworks** — Research **Great Expectations** and **Databricks' native `CONSTRAINT` checks** in Delta Live Tables. For this project, hand-written assertions are fine, but know that production systems use dedicated DQ frameworks.
- **Delta Live Tables (DLT)** — Databricks' declarative pipeline framework that automates orchestration, quality checks, and lineage. It's beyond this project's scope but worth knowing it exists and that it's the *next step* after mastering manual pipelines.

---

## Phase 6 — Advanced Topics & Production Polish ⏱️ ~2 hrs

### 6.1 — Implement Table Optimisation

- [ ] Run `OPTIMIZE` on your Gold tables
- [ ] Run `VACUUM` to remove old Delta transaction log files
- [ ] Add `OPTIMIZE` and `VACUUM` as a task in your Job DAG

**📚 Research Scope:**
- **`OPTIMIZE`** — Delta tables accumulate many small Parquet files over time (the "small files problem"). `OPTIMIZE` compacts them into larger files, dramatically improving read performance. Run it regularly on frequently-written tables.
- **`VACUUM`** — Deletes Parquet files that are no longer referenced by the Delta transaction log (files older than the retention period, default 7 days). This reclaims storage. **Important:** `VACUUM` permanently removes the ability to time-travel before the retention threshold. Don't run it on Bronze tables if you want long time-travel history.
- **`ZORDER BY`** — Run `OPTIMIZE table ZORDER BY (date, commodity_id)` to co-locate data by your most common filter columns. This enables **data skipping** — Delta's statistics allow it to skip entire files that don't contain matching values.
- **Liquid Clustering** (DBR 13.3+) — The modern replacement for `ZORDER`. Research it as the future direction.

---

### 6.2 — Explore Delta Lake Time Travel

- [ ] Query your Bronze table as it was 2 hours ago: `SELECT * FROM table TIMESTAMP AS OF ...`
- [ ] Roll back a Silver table to a previous version after simulating a bad write: `RESTORE TABLE ... TO VERSION AS OF ...`
- [ ] Query the Delta transaction log: `DESCRIBE HISTORY table`

**📚 Research Scope:**
- **Delta transaction log (`_delta_log`)** — Every write to a Delta table creates a new JSON file in `_delta_log`. These files contain the exact operations performed (add file, remove file, metadata change). The log is the source of truth for ACID guarantees and time travel.
- **`DESCRIBE HISTORY`** — Returns all historical operations on a table: writes, merges, optimises, vacuums. Use this to audit your pipeline runs.
- **ACID Transactions in Delta** — Understand what Atomicity, Consistency, Isolation, and Durability mean in the context of distributed storage. Delta achieves ACID by using optimistic concurrency control via the transaction log.

---

### 6.3 — Security & Governance Basics

- [ ] Review Unity Catalog **privileges**: `USE CATALOG`, `USE SCHEMA`, `SELECT`, `MODIFY`
- [ ] Understand what a **Service Principal** is and when you'd use one for job execution
- [ ] Tag your Gold tables with metadata: `ALTER TABLE ... SET TAGS ('layer' = 'gold', 'domain' = 'commodities')`

**📚 Research Scope:**
- **Unity Catalog vs legacy Hive Metastore** — Older Databricks workspaces use the Hive Metastore for table metadata. Unity Catalog is the modern replacement with fine-grained access control, lineage tracking, and multi-workspace governance. Understand why the migration is happening.
- **Data lineage** — Unity Catalog automatically tracks which tables were derived from which other tables. Navigate to a Gold table in Catalog Explorer and view its lineage graph. This is invaluable for impact analysis ("if I change Silver table X, what Gold tables and dashboards are affected?").
- **Column-level security and row-level filters** — Advanced UC features to restrict access to sensitive columns or rows. Worth knowing they exist.

---

## Phase 7 — Review, Documentation & Reflection ⏱️ ~0.5 hrs

### 7.1 — Final Review

- [ ] End-to-end run: drop all tables, re-run the full pipeline from Bronze to Gold, verify the dashboard
- [ ] Review your `databricks.yml` — is everything defined as code? Nothing was manually created via UI that isn't tracked?
- [ ] Verify all notebooks are committed to GitHub via Databricks Repos

### 7.2 — Write a README

- [ ] Document: Architecture diagram (even a simple ASCII one), how to set up and run the project, what each notebook does, known limitations

### 7.3 — Reflection Prompts

Answer these in your README or a personal notes file to solidify your learning:

- [ ] Why did I use `MERGE` at Silver instead of `INSERT OVERWRITE`?
- [ ] What would break first if 10x the data volume arrived tomorrow?
- [ ] How would I add streaming ingestion to the Bronze layer?
- [ ] What's the difference between a Delta checkpoint and Delta time travel?
- [ ] Why is the Gold layer a star schema instead of a normalised model?
- [ ] What would I use Delta Live Tables for if I rebuilt this project?

---

## Appendix — Key Concept Quick Reference

| Concept | Layer | Why It Matters |
|---|---|---|
| Delta Lake | All | ACID, time travel, schema enforcement |
| Medallion Architecture | All | Separation of concerns, iterative refinement |
| Auto Loader | Bronze | Scalable, incremental file ingestion |
| Schema Evolution (`mergeSchema`) | Bronze/Silver | Handle new columns without breaking pipelines |
| `MERGE INTO` (Upsert) | Silver/Gold | Idempotent writes, SCD Type 2, late arrivals |
| Window Functions | Silver/Gold | Time series calculations (rolling avg, returns) |
| Star Schema | Gold | Optimised for analytical queries and BI tools |
| `OPTIMIZE` + `ZORDER` | Gold | Query performance, small file compaction |
| `VACUUM` | All | Storage reclamation |
| Time Travel | Bronze/Silver | Auditability, rollback, debugging |
| Unity Catalog | All | Governance, lineage, access control |
| DAB (`databricks.yml`) | Pipeline | IaC, repeatable deployments |
| SQL Warehouse (Photon) | Dashboard | Fast, cost-efficient BI queries |
| Job DAG + `depends_on` | Orchestration | Reliable, scheduled pipeline execution |
| Idempotency | All | Safe re-runs, resilient pipelines |

---

## Hour Budget Summary

| Phase | Description | Est. Hours |
|---|---|---|
| 0 | Setup & Environment | 1.5 |
| 1 | Bronze Ingestion | 2.5 |
| 2 | Silver Transformation | 2.5 |
| 3 | Gold Aggregation | 2.5 |
| 4 | SQL & Dashboard | 2.0 |
| 5 | Orchestration | 1.5 |
| 6 | Advanced / Polish | 2.0 |
| 7 | Review & Docs | 0.5 |
| **Total** | | **~15 hrs** |

---

*Built for learning. Break things deliberately, read the error, understand the fix. That's the curriculum.*
