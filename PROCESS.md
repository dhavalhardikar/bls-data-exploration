# BLS Public Data Exploration Process

This repository explores the U.S. Bureau of Labor Statistics (BLS) public time-series data by ingesting raw files into a Databricks Volume and then transforming them through a Bronze → Silver → Gold medallion pipeline.

## 1. Overview

The data exploration pipeline has three main stages:

1. **Ingestion**: download BLS raw files into a Databricks Volume using `src/bls_pipeline.py`.
2. **Bronze**: read raw files from the Volume into Bronze tables with minimal sanitization.
3. **Silver**: clean, type-cast, and validate fields to produce usable table structures.
4. **Gold**: compute analytic outputs and exploratory deliverables.

This setup is orchestrated from `driver_notebook.py`, which documents the configuration and runs the pipeline.

## 2. Key Files

- `driver_notebook.py`
  - Orchestration notebook that documents the pipeline configuration and triggers the Databricks declarative pipeline run.
  - Sets the target catalog, schema, volume, and pipeline ID.
  - Contains example validation queries for Gold outputs.

- `src/bls_pipeline.py`
  - Handles raw BLS file ingestion from `https://download.bls.gov/pub/time.series/`.
  - discovers survey directories, downloads files, and writes them into the configured Volume root.
  - includes retry logic, HEAD-size checks, and polite request pacing.

- `medallion_sdp/transformations/pipeline_config.py`
  - Centralizes pipeline configuration keys for catalog/schema/volume.
  - Defines the relative paths to the ingested BLS files used by the Bronze layer.

- `medallion_sdp/transformations/bronze.py`
  - Reads raw BLS TSV and Population JSON file from the Volume.
  - Sanitizes column names to make them Delta-compatible.
  - Produces raw Bronze artifacts for data, series metadata, sector lookup, measure lookup, and Census population JSON.

- `medallion_sdp/transformations/silver.py`
  - Cleans and normalizes Bronze tables.
  - Casts types, trims strings, and applies pipeline expectations.
  - Produces structured Silver datasets for BLS time series, series metadata, sector/measure lookup, and population.

- `medallion_sdp/transformations/gold.py`
  - Builds analytic Gold tables on top of Silver.
  - Produces high-level deliverables like population stats, best year per series, and series population comparisons.

- `src/gold_reference.py`
  - Plain PySpark reference implementations of the Gold logic.
  - Useful for unit tests and notebook validation outside the declarative pipeline runtime.

## 3. Ingestion Step

### Raw source

The ingestion code targets the BLS public time-series root:

- `https://download.bls.gov/pub/time.series/`

This directory contains:

- `pr/` survey data files such as `pr.data.1.AllData`, `pr.series`, `pr.sector`, `pr.measure`
- `population/bls_population_data_api.json`

### Workflow

1. `driver_notebook.py` sets:
   - `CATALOG = "bls"`
   - `SCHEMA = "bls_time_series"`
   - `VOLUME = "bls_raw_files"`
   - `VOLUME_ROOT = "/Volumes/bls/bls_time_series/bls_raw_files"`

2. `src/bls_pipeline.py` does the following:
   - Creates a retrying HTTP session with `create_retry_session()`.
   - Lists top-level survey directories by scraping HTML directory indexes.
   - Filters out non-survey items like `overview.txt`, `compressed`, and `sdmx`.
   - Downloads each survey file into `<volume_root>/<survey_code>/...`.
   - Uses a `HEAD` request to compare remote `Content-Length` with local file size to skip unchanged downloads.
   - Sleeps between requests to avoid rate limiting.

3. The result is a local Volume containing raw BLS TSV/JSON files.

## 4. Bronze Layer

The Bronze transformation files ingest raw source files with minimal processing.

### `bronze_bls_data`

- Reads `pr/pr.data.1.AllData` as tab-delimited text.
- Keeps values as raw strings and sanitizes column names.

### `bronze_bls_series`

- Reads `pr/pr.series` metadata.
- Preserves the original survey-provided columns.

### `bronze_bls_sector`

- Reads lookup codes from `pr/pr.sector`.
- Normalizes column names and values.

### `bronze_bls_measure`

- Reads lookup codes from `pr/pr.measure`.

### `bronze_population`

- Reads `population/bls_population_data_api.json` as JSON.
- Explodes the nested `data` array into rows.
- Sanitizes resulting JSON field names to Delta-safe column names.

## 5. Silver Layer

The Silver layer cleans Bronze outputs and enforces expected schema quality.

### `silver_bls_data`

- Reads Bronze time-series data.
- Trims `series_id`, `period`, and `footnote_codes`.
- Casts `year` to `INT` and `value` to `DOUBLE`.
- Drops rows missing `series_id`, `year`, or `value`.
- Flags rows with unexpected `period` format.

### `silver_bls_series`

- Reads Bronze series metadata.
- Trims all string columns to make joins and lookups consistent.

### `silver_bls_sector` / `silver_bls_measure`

- Read the two-column lookup tables positionally from BLS raw data.
- Normalize code and label columns to `sector_code`, `sector_name`, `measure_code`, and `measure_text`.

### `silver_population`

- Reads Bronze population records.
- Casts `Year` to `INT` and `Population` to `BIGINT`.
- Drops rows missing `year` or `population`.

## 6. Gold Layer

The Gold layer produces analytical outputs built from Silver tables.

### `gold_population_stats_2013_2018`

- Filters population data for years 2013 through 2018.
- Computes mean and standard deviation of the U.S. population.

### `gold_best_year_per_series`

- Uses quarterly periods `Q01`–`Q04`.
- Aggregates total quarterly `value` per `series_id` and `year`.
- Selects the year with the highest total value for each series.
- Joins series metadata with sector and measure labels.
- Synthesizes `series_title` from `sector_name` and `measure_text`.

### `gold_prs30006032_q1_population`

- Filters Silver BLS data for `series_id = PRS30006032` and `period = Q01`.
- Joins these quarterly values with national population by year.

## 7. Running the Pipeline

### Notebook-driven run

Use `driver_notebook.py` as the control-plane document:

- Confirm the pipeline configuration values match the Databricks pipeline settings.
- Run the ingestion cell to sync raw files.
- Start the Databricks pipeline update and poll for completion.
- Run spot-check queries against Gold tables.

### Pipeline configuration

`medallion_sdp/transformations/pipeline_config.py` ensures the pipeline reads the same volume path that ingestion writes to.

The declarative pipeline expects configuration keys:

- `bls_pipeline.catalog`
- `bls_pipeline.schema`
- `bls_pipeline.volume`

## 8. Notes and Validation

- `gold_reference.py` provides plain PySpark implementations for the Gold analytics.
- This reference module is useful for validation and testing outside the Databricks pipeline.
- The declarative `gold.py` file intentionally mirrors the same logic so results can be compared.

## 9. Summary

This repository explores BLS public data by:

- downloading raw BLS survey files and population JSON,
- landing them into a Databricks Volume,
- reading the raw data into Bronze tables,
- cleaning and normalizing into Silver tables,
- producing analytics in Gold tables.

The process is intentionally layered so raw source preservation, data quality, and analytical deliverables are clearly separated.
