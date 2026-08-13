# PROCESS.md

## Architecture

### Why Bronze / Silver / Gold at all

This Medallion architechture is a Data design pattern which progressively improve data quality with 
Bronze (raw), Silver(clean) and Gold (aggregated)
provides structure and clear lineage, 

The 2 source systems: 
1. BLS's `/pr/` flat-file directory - tab-delimited fixed-format text scraped
off an HTML directory listing, and
2. DataUSA population API — one is , the other is nested JSON from a REST endpoint.
 
2 seperate types of datasets brings challenges like 
unexpected behavior or parser inconsistency. Splitting into three layers keeps each concern isolated:

- **Bronze** — land the data as-is, with bare minimum transformation needed to
  make it *storable*. Sanatize column names to remove special characters and explode columns names of population json "data".

- **Silver** — this is where "raw bytes" become "trustworthy rows": trimming 
  strings, casting `year`/`value`/`population` to integer types.

- **Gold** — pure business logic on top of already-clean, already-typed data:
  aggregations, joins, and the analytical deliverables (population stats,
  best-year-per-series, and the target-series-vs-population join). 


### Modular approach
It allows you to break down monolith notebooks(driver_notebook.py) into reusable components,(src.bls_pipline.py) which simplifies debugging, enhances testing, and eliminates code duplication.

`bls_pipeline.py`'s scraping/HEAD-check/retry-session logic runs in the **driver
notebook**

Ingestion kept outside the declarative DAG: scraping/retries/idempotent syncs — ran as a plain pre-step in the driver notebook, then the pipeline triggers, only once data has safely landed.

### Why PySpark DataFrame API is primary in Gold, with Spark SQL as a secondary file

`gold.py`: The
DataFrame version is primary because:

- It's unit-testable, which matters more for
  Gold than Bronze/Silver because Gold is where business-logic bugs can be most expensive.
- No need of string interpolation for sql query, easy use of plain Python helpers and constants (`QUARTER_PERIODS`,
  `TARGET_SERIES_ID`) 



## Trade-offs

Things I'd handle differently if this were headed to a real client rather than a
scoped refactor:

- **Schema drift.** For a real client schema validation I'd add an explicit expected-schema check at
  Bronze (compare `df.schema` against a versioned expected schema, either failing loud
  or routing to a quarantine table) rather than discovering drift three layers later.

- **Data volume.** For data at scale
  production volume Auto Loader (`cloudFiles`)  should be considered for incremental load,
  checkpointed file discovery and would look at whether the population API needs pagination handling.

- **Access control.** `CATALOG`/`SCHEMA`/`VOLUME` are plain constants with no
  Unity Catalog grant strategy attached. For a real client 
  environment specific RBAC should be added.

- **Monitoring.** Will use `logger()` instead of `print()` statements at info level to raise exceptions at necessary junctures in tandem with try catch block..
For a real client I'd add audit log tables with 
metrics table (or push to the pipeline's event log / a monitoring system) and wire an alert for expectation-failure rate crossing a threshold or in case of failure.


## Retrospective


**Understanding the ingestion ask itself took time**
- Wasn't immediately obvious why two separate API integrations were needed — took time to realize BLS /pr/ is flat-file time-series data while DataUSA is a separate REST API for population, each with different shapes and sync strategies.
- The 403 was quick to fix since it was already flagged, but the rate-limiting issue took multiple debugging iterations to resolve — turned out to be BLS's bot-protection throttling too-frequent requests (bls.gov/bls/pss.htm), fixed with a polite delay + retry/backoff session.

**Best-year-per-series query was the toughest analytical piece**
- The ambiguity was in "human-readable label" — had to use AI assistance to interpret what that meant in the context of BLS's coded `pr.series` format.
- Drilling into how `series`, `sector`, and `measure` lookup files relate back to `pr.data.1.AllData` was key to correctly deriving the join criteria (`series_id → sector_code/measure_code → labels`).
