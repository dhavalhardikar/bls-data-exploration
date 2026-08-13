# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## BLS & Population Bronze -> Silver -> Gold — Orchestration Driver
# MAGIC
# MAGIC This notebook sets the pipeline's configuration and triggers a run of the 
# MAGIC Spark Declarative Pipeline. Declarative pipeline transformation files execute inside 
# MAGIC the *pipeline's own* Spark session.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Create the catalog
# MAGIC CREATE CATALOG IF NOT EXISTS bls;
# MAGIC
# MAGIC -- 2. Create the schema inside the catalog
# MAGIC CREATE SCHEMA IF NOT EXISTS bls.bls_time_series;
# MAGIC
# MAGIC -- 3. Create the volume inside the schema
# MAGIC CREATE VOLUME IF NOT EXISTS bls.bls_time_series.bls_raw_files;

# COMMAND ----------

CATALOG = "bls"
SCHEMA = "bls_time_series"
VOLUME = "bls_raw_files"
# ETL pipeline ID which will trigger Databricks declarative pipeline
PIPELINE_ID = "c19606ab-f63b-487c-b555-6641c2d1a561"

VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
print(f"Target: {CATALOG}.{SCHEMA} | Volume root: {VOLUME_ROOT} | Pipeline ID: {PIPELINE_ID or '(not set)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 — Preserve and Extend Ingestion
# MAGIC Trigger idempotent syncs for both BLS /pr/ files and DataUSA population records to land raw files
# MAGIC in the Volume before the declarative pipeline reads them.

# COMMAND ----------

from src.bls_pipeline import create_retry_session, run_ingestion

HEADERS = {
    "User-Agent": "DataEngineeringTeam dev-contact@mycompany.com",
    "Accept-Encoding": "identity",
}

# Creates request session object so that headers and connection pooling can be reused across multiple requests
session = create_retry_session(headers=HEADERS)

BLS_PR_URL = "https://download.bls.gov/pub/time.series/pr/"
POPULATION_API_URL = "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"

ingestion_stats = run_ingestion(
    session=session,
    bls_pr_url=BLS_PR_URL,
    population_api_url=POPULATION_API_URL,
    volume_root=VOLUME_ROOT,
    request_delay=0.5,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 — Trigger the declarative pipeline run
# MAGIC Triggers the update on the declarative pipelines resolving from the updated Volume paths.

# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient

if not PIPELINE_ID:
    raise ValueError("Set the 'pipeline_id' to the target Pipeline's ID before running this cell.")

w = WorkspaceClient()
update = w.pipelines.start_update(pipeline_id=PIPELINE_ID)
print(f"Started update {update.update_id} for pipeline {PIPELINE_ID}")

TERMINAL_STATES = {"COMPLETED", "FAILED", "CANCELED"}
state = "UNKNOWN"

while state not in TERMINAL_STATES:
    info = w.pipelines.get_update(pipeline_id=PIPELINE_ID, update_id=update.update_id)
    state = info.update.state.value if info.update.state else "UNKNOWN"
    print(f"Update state: {state}")
    if state not in TERMINAL_STATES:
        # Wait for 15 seconds before checking the state again
        time.sleep(15)

if state != "COMPLETED":
    raise RuntimeError(f"Pipeline update ended in state '{state}'. Check the pipeline event log for details.")

print("Pipeline run completed successfully — Bronze, Silver, and Gold tables are refreshed.")
