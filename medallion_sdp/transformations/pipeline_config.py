"""Shared configuration for the BLS Bronze -> Silver -> Gold declarative pipeline.

Declarative Pipeline transformation files (pipelines/bronze.py, silver.py, gold.py)
run inside the pipeline's own Spark session, not inside a notebook's — so they can't
just import a Python constant set by a driver notebook at "run time" the way normal
modules can. Instead they read three keys from **pipeline configuration**
(Settings -> Configuration in the Pipeline UI, or the `configuration:` block in
pipeline.yml), which is the supported way to parameterize a declarative pipeline:

    bls_pipeline.catalog   (default: bls)
    bls_pipeline.schema    (default: bls_time_series)
    bls_pipeline.volume    (default: bls_raw_files)

This module centralizes that lookup + the resulting source paths so every
transformation file references the same constants instead of re-deriving them.
"""

CATALOG_CONF_KEY = "bls_pipeline.catalog"
SCHEMA_CONF_KEY = "bls_pipeline.schema"
VOLUME_CONF_KEY = "bls_pipeline.volume"

DEFAULT_CATALOG = "bls"
DEFAULT_SCHEMA = "bls_time_series"
DEFAULT_VOLUME = "bls_raw_files"


def resolve_volume_root(spark) -> str:
    """Reads catalog/schema/volume from pipeline configuration and builds the Volume root path."""
    catalog = spark.conf.get(CATALOG_CONF_KEY, DEFAULT_CATALOG)
    schema = spark.conf.get(SCHEMA_CONF_KEY, DEFAULT_SCHEMA)
    volume = spark.conf.get(VOLUME_CONF_KEY, DEFAULT_VOLUME)
    return f"/Volumes/{catalog}/{schema}/{volume}"


# Relative locations of the raw files within the Volume — preserved from the
# existing ingestion routine (bls_pipeline.py / run_full_bls_ingestion), which
# syncs every file under "<volume_root>/<survey_code>/", including these
# small BLS lookup/mapping files alongside pr.data.1.AllData and pr.series.
BLS_DATA_RELATIVE_PATH = "pr/pr.data.1.AllData"
BLS_SERIES_RELATIVE_PATH = "pr/pr.series"
BLS_SECTOR_RELATIVE_PATH = "pr/pr.sector"
BLS_MEASURE_RELATIVE_PATH = "pr/pr.measure"
POPULATION_JSON_RELATIVE_PATH = "population/bls_population_data_api.json"
