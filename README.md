# BLS Productivity & Population — Bronze/Silver/Gold Pipeline

A Databricks **Spark Declarative Pipeline** that ingests BLS Productivity & Costs
(`pr`) time series and Census/DataUSA population data, then transforms them through
a Bronze → Silver → Gold medallion architecture.

## End-to-end flow

```
driver_notebook.py
  │
  ├─ 1. Ensure catalog/schema/volume exist (SQL)
  │
  ├─ 2. run_ingestion()  [bls_pipeline.py]
  │      ├─ scrape BLS /pr/ directory → download changed files (HEAD size check)
  │      └─ fetch DataUSA population JSON → overwrite on content-hash change
  │      → raw files land in /Volumes/{catalog}/{schema}/{volume}/{pr,population}/
  │
  └─ 3. Trigger the declarative pipeline run (WorkspaceClient.pipelines.start_update)
         and poll until COMPLETED/FAILED/CANCELED
              │
              ▼
   Spark Declarative Pipeline (its own Spark session)
              │
   bronze.py  →  silver.py  →  gold.py
```

The notebook is the **orchestrator**: it lands fresh raw files in the Volume, then
kicks off a pipeline update. The pipeline transformation files (`bronze.py`,
`silver.py`, `gold.py`) run separately, inside the pipeline's own Spark session —
they never import the notebook or `bls_pipeline.py` directly; they only read from
pipeline configuration and from tables registered via `@dp.table`.

## Modules

| File | Layer | Responsibility |
|---|---|---|
| `bls_pipeline.py` | Ingestion | Scrapes the BLS `/pr/` directory and syncs the Population API into the Volume, idempotently (size/hash checks skip unchanged files). Pure `requests`/`bs4`, no Spark. |
| `driver_notebook.py` | Orchestration | Databricks notebook: provisions catalog/schema/volume, calls `run_ingestion`, then triggers and monitors the declarative pipeline run via the Databricks SDK. |
| `pipeline_config.py` | Config | Resolves `catalog`/`schema`/`volume` from pipeline configuration (with defaults) and centralizes the raw file paths within the Volume. Imported by `bronze.py`. |
| `bronze.py` | Bronze | Reads raw BLS TSV files and the population JSON exactly as they land, with no cleansing beyond making column names Delta-safe (sanitizing spaces/punctuation). One `@dp.table` per raw source. |
| `silver.py` | Silver | Cleanses/types Bronze output (trim strings, cast year/value/population) and applies data-quality rules natively (`.filter(...)` for hard/drop rules; count-and-log for soft/flag rules) — no proprietary `@dp.expect*` decorators. |
| `gold.py` | Gold | Analytical tables built from Silver: population mean/stddev (2013–2018), best year per BLS series (with resolved sector/measure labels), and a target series' Q1 values joined to population. |

## Table lineage

```
bronze_bls_data ────────┐
bronze_bls_series ───┐  │
bronze_bls_sector ─┐ │  │        silver_bls_data ──┐
bronze_bls_measure┐│ │  │        silver_bls_series ┤
                  ││ │  │        silver_bls_sector ┤──> gold_best_year_per_series
                  ▼▼ ▼  ▼        silver_bls_measure┘
             silver_bls_measure/sector/series/data
bronze_population ──> silver_population ──> gold_population_stats_2013_2018
                                        └──> gold_prs30006032_q1_population
                                             (joined with silver_bls_data)
```

Each Silver/Gold table reads its inputs via `dp.read("<table_name>")`, so the
dependency graph above is inferred automatically by the pipeline runtime.

## Key design notes

- **Config, not hardcoded paths**: `bronze.py` resolves the Volume root from
  pipeline configuration (`bls_pipeline.catalog/schema/volume`) via
  `pipeline_config.resolve_volume_root`, since transformation files run in a
  separate Spark session from the driver notebook and can't share Python state.
- **Delta-safe column names**: BLS/Census raw files can contain spaces or
  punctuation in headers; `bronze.py` sanitizes these at ingestion so nothing
  downstream needs to special-case them.
- **Native data quality**: Silver enforces the same hard-drop / soft-flag
  semantics as Databricks' `@dp.expect_or_drop` / `@dp.expect`, but with plain
  PySpark (`.filter()` + count-and-log), keeping the logic open-source-portable.
- **Idempotent ingestion**: `bls_pipeline.py` only downloads a BLS file when its
  size differs from what's stored, and only overwrites the population JSON when
  its content hash changes — safe to re-run on a schedule.
