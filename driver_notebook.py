# Databricks notebook source
# MAGIC %md
# MAGIC # BLS Bronze -> Silver -> Gold — Orchestration Driver
# MAGIC
# MAGIC This notebook is **not** part of the declarative pipeline graph — it's the
# MAGIC control-plane notebook that documents/sets the pipeline's configuration and
# MAGIC triggers a run of the Spark Declarative Pipeline defined in `pipelines/bronze.py`,
# MAGIC `pipelines/silver.py`, and `pipelines/gold.py` (spec: `pipelines/pipeline.yml`).
# MAGIC
# MAGIC Declarative pipeline transformation files execute inside the *pipeline's own*
# MAGIC Spark session, not this notebook's — so they can't read a Python variable set here.
# MAGIC They read `bls_pipeline.catalog` / `bls_pipeline.schema` / `bls_pipeline.volume`
# MAGIC from **pipeline configuration** instead (see `pipelines/pipeline_config.py`).
# MAGIC Make sure the values below match what's set under the pipeline's Configuration —
# MAGIC this notebook doesn't push them there; it just needs to agree with them so the
# MAGIC ingestion step (existing `bls_pipeline.py` routine) and the declarative pipeline
# MAGIC are pointed at the same Volume.

# COMMAND ----------

CATALOG = "bls"
SCHEMA = "bls_time_series"
VOLUME = "bls_raw_files"
PIPELINE_ID = "c19606ab-f63b-487c-b555-6641c2d1a561"

VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
print(f"Target: {CATALOG}.{SCHEMA} | Volume root: {VOLUME_ROOT} | Pipeline ID: {PIPELINE_ID or '(not set)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 — Preserve existing ingestion
# MAGIC Unchanged: reuses the existing `bls_pipeline.py` sync routine to land raw files
# MAGIC in the Volume before the declarative pipeline reads them.

# COMMAND ----------

# MAGIC %skip
# MAGIC from src.bls_pipeline import create_retry_session, run_full_bls_ingestion
# MAGIC
# MAGIC HEADERS = {
# MAGIC     "User-Agent": "DataEngineeringTeam dev-contact@mycompany.com",
# MAGIC     "Accept-Encoding": "identity",
# MAGIC }
# MAGIC
# MAGIC session = create_retry_session(headers=HEADERS)
# MAGIC
# MAGIC ingestion_stats = run_full_bls_ingestion(
# MAGIC     session=session,
# MAGIC     base_url="https://download.bls.gov/pub/time.series/",
# MAGIC     volume_root=VOLUME_ROOT,
# MAGIC     request_delay=0.5,
# MAGIC     limit_surveys=None,  # full run; set to an int to test against a handful of surveys
# MAGIC )
# MAGIC print(ingestion_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 — Trigger the declarative pipeline run
# MAGIC The pipeline itself must already exist (created via UI or Asset Bundle) pointing
# MAGIC at the `pipelines/` folder, with its Configuration section set to the same
# MAGIC catalog/schema/volume values as above. This cell just starts an update and
# MAGIC polls it to completion — it doesn't define the pipeline.

# COMMAND ----------

import time

from databricks.sdk import WorkspaceClient

if not PIPELINE_ID:
    raise ValueError("Set the 'pipeline_id' to the target Lakeflow Pipeline's ID before running this cell.")

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
        time.sleep(15)

if state != "COMPLETED":
    raise RuntimeError(f"Pipeline update ended in state '{state}'. Check the pipeline event log for details.")

print("Pipeline run completed successfully — Bronze, Silver, and Gold tables are refreshed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 — Spot-check Gold output

# COMMAND ----------

spark.table(f"{CATALOG}.{SCHEMA}.gold_population_stats_2013_2018").show(truncate=False)

# COMMAND ----------

spark.table(f"{CATALOG}.{SCHEMA}.gold_best_year_per_series").show(10, truncate=False)

# COMMAND ----------

spark.table(f"{CATALOG}.{SCHEMA}.gold_prs30006032_q1_population").show(truncate=False)