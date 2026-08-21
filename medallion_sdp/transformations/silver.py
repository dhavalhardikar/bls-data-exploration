"""Silver layer: cleansing, type normalization, and data quality expectations.

Refactored to use native, open-source PySpark DataFrame operations. 
@dp.table / dp.read are the Spark Declarative Pipeline 
framework's core table-registration API.

Data-quality semantics are preserved exactly:
  - Hard rules: rows failing the rule are
    dropped from the table's output, via a plain .filter(...) call.
"""

import pyspark.pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dp.table(
    name="silver_bls_data",
    comment="Cleansed BLS time series values: trimmed identifiers, typed year/value, quality-checked.",
)
def silver_bls_data() -> DataFrame:
    df = dp.read("bronze_bls_data")
    cleansed = df.select(
        F.trim(F.col("series_id")).alias("series_id"),
        F.col("year").cast("INT").alias("year"),
        F.trim(F.col("period")).alias("period"),
        F.col("value").cast("DOUBLE").alias("value"),
        F.trim(F.col("footnote_codes")).alias("footnote_codes"),
    )

    # --- Hard rules
    # — rows failing any of these are dropped.
    validated = cleansed.filter(
        F.col("series_id").isNotNull()
        & F.col("year").isNotNull()
        & F.col("value").isNotNull()
    )

    return validated


@dp.table(
    name="silver_bls_series",
    comment="Cleansed BLS series metadata: trimmed text fields for reliable joins/display.",
)
def silver_bls_series() -> DataFrame:
    df = dp.read("bronze_bls_series")
    trimmed = [F.trim(F.col(c)).alias(c) for c in df.columns if df.schema[c].dataType.typeName() == "string"]
    other = [F.col(c) for c in df.columns if df.schema[c].dataType.typeName() != "string"]
    cleansed = df.select(*trimmed, *other)

    # --- Hard rule ---
    return cleansed.filter(F.col("series_id").isNotNull())


def _read_code_label_lookup(table_name: str, code_alias: str, label_alias: str) -> DataFrame:
    """
    Reads a small BLS '<code>, <label>, ...' lookup table (e.g. pr.sector, pr.measure)
    positionally: column 0 is always the code, column 1 is always the human-readable
    label — but BLS doesn't use a consistent literal name for that label column across
    surveys (e.g. 'sector_name' in some, 'sector_text' in others), so we read by
    position instead of guessing a name.
    """
    df = dp.read(table_name)
    code_col, label_col = df.columns[0], df.columns[1]
    return df.select(
        F.trim(F.col(code_col)).alias(code_alias),
        F.trim(F.col(label_col)).alias(label_alias),
    )


@dp.table(
    name="silver_bls_sector",
    comment="Cleansed sector_code -> sector label lookup.",
)
def silver_bls_sector() -> DataFrame:
    return _read_code_label_lookup("bronze_bls_sector", "sector_code", "sector_name")


@dp.table(
    name="silver_bls_measure",
    comment="Cleansed measure_code -> measure label lookup.",
)
def silver_bls_measure() -> DataFrame:
    return _read_code_label_lookup("bronze_bls_measure", "measure_code", "measure_text")


@dp.table(
    name="silver_population",
    comment="Cleansed Census ACS national population by year.",
)
def silver_population() -> DataFrame:
    df = dp.read("bronze_population")
    cleansed = df.select(
        F.trim(F.col("Nation_ID")).alias("nation_id"),
        F.trim(F.col("Nation")).alias("nation"),
        F.col("Year").cast("INT").alias("year"),
        F.col("Population").cast("BIGINT").alias("population"),
    )

    # --- Hard rules ---
    # rows failing are dropped.
    return cleansed.filter(
        F.col("year").isNotNull() & F.col("population").isNotNull()
    )