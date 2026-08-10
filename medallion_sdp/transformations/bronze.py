"""Bronze layer: raw ingestion, no cleansing or type-casting.

Reads the same raw files produced by the existing ingestion routine
(bls_pipeline.run_full_bls_ingestion) straight off the Volume, as-is.
"""

import re

import pyspark.pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipeline_config import (
    BLS_DATA_RELATIVE_PATH,
    BLS_MEASURE_RELATIVE_PATH,
    BLS_SECTOR_RELATIVE_PATH,
    BLS_SERIES_RELATIVE_PATH,
    POPULATION_JSON_RELATIVE_PATH,
    resolve_volume_root,
)

# Delta rejects these characters in column names outright unless Column Mapping is
# turned on (space, comma, semicolon, braces, parens, newline, tab, equals) — see
# DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES. Rather than enabling Column Mapping (extra
# protocol complexity, some interop tradeoffs), every Bronze table is sanitized to a
# Delta-safe schema at ingestion time, so nothing downstream can trip on it again.
_INVALID_DELTA_COLUMN_CHARS = re.compile(r"[ ,;{}()\n\t=]")


def _sanitize_column_names(df: DataFrame) -> DataFrame:
    """Replaces Delta-invalid characters in column names with underscores."""
    renamed = [F.col(f"`{c}`").alias(_INVALID_DELTA_COLUMN_CHARS.sub("_", c)) for c in df.columns]
    return df.select(*renamed)


def _read_bls_tsv(path: str) -> DataFrame:
    """Reads a tab-delimited BLS flat file and strips whitespace BLS pads header names with."""
    df = spark.read.option("header", True).option("sep", "\t").csv(path)
    df = df.toDF(*[c.strip() for c in df.columns])
    return _sanitize_column_names(df)


@dp.table(
    name="bronze_bls_data",
    comment="Raw BLS Productivity & Costs (pr) time series values, ingested as-is from pr.data.1.AllData.",
)
def bronze_bls_data() -> DataFrame:
    volume_root = resolve_volume_root(spark)
    return _read_bls_tsv(f"{volume_root}/{BLS_DATA_RELATIVE_PATH}")


@dp.table(
    name="bronze_bls_series",
    comment="Raw BLS series metadata (sector/class/measure/duration codes), ingested as-is from pr.series.",
)
def bronze_bls_series() -> DataFrame:
    volume_root = resolve_volume_root(spark)
    return _read_bls_tsv(f"{volume_root}/{BLS_SERIES_RELATIVE_PATH}")


@dp.table(
    name="bronze_bls_sector",
    comment="Raw BLS sector code -> sector label lookup, ingested as-is from pr.sector.",
)
def bronze_bls_sector() -> DataFrame:
    volume_root = resolve_volume_root(spark)
    return _read_bls_tsv(f"{volume_root}/{BLS_SECTOR_RELATIVE_PATH}")


@dp.table(
    name="bronze_bls_measure",
    comment="Raw BLS measure code -> measure label lookup, ingested as-is from pr.measure.",
)
def bronze_bls_measure() -> DataFrame:
    volume_root = resolve_volume_root(spark)
    return _read_bls_tsv(f"{volume_root}/{BLS_MEASURE_RELATIVE_PATH}")


@dp.table(
    name="bronze_population",
    comment="Raw Census ACS population records, exploded from the nested 'data' array in the API JSON payload.",
)
def bronze_population() -> DataFrame:
    volume_root = resolve_volume_root(spark)
    raw = spark.read.option("multiLine", True).json(f"{volume_root}/{POPULATION_JSON_RELATIVE_PATH}")
    exploded = raw.select(F.explode(F.col("data")).alias("row")).select("row.*")
    return _sanitize_column_names(exploded)  # "Nation ID" -> "Nation_ID", etc.