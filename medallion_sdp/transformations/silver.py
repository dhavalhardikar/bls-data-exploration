"""Silver layer: cleansing, type normalization, and data quality expectations.

@dp.expect_or_drop rows failing a hard rule (missing keys/values) are dropped from
the output. @dp.expect flags rows failing a soft rule (e.g. an unexpected period
code) in pipeline metrics without dropping them, since a slightly odd period code
shouldn't block otherwise-valid rows from reaching Gold.
"""

import pyspark.pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dp.table(
    name="silver_bls_data",
    comment="Cleansed BLS time series values: trimmed identifiers, typed year/value, quality-checked.",
)
@dp.expect_or_drop("series_id_present", "series_id IS NOT NULL")
@dp.expect_or_drop("year_present", "year IS NOT NULL")
@dp.expect_or_drop("value_present", "value IS NOT NULL")
@dp.expect("period_format_valid", "period RLIKE '^(Q0[1-5]|M(0[1-9]|1[0-2]))$'")
def silver_bls_data() -> DataFrame:
    df = dp.read("bronze_bls_data")
    return df.select(
        F.trim(F.col("series_id")).alias("series_id"),
        F.col("year").cast("INT").alias("year"),
        F.trim(F.col("period")).alias("period"),
        F.col("value").cast("DOUBLE").alias("value"),
        F.trim(F.col("footnote_codes")).alias("footnote_codes"),
    )


@dp.table(
    name="silver_bls_series",
    comment="Cleansed BLS series metadata: trimmed text fields for reliable joins/display.",
)
@dp.expect_or_drop("series_id_present", "series_id IS NOT NULL")
def silver_bls_series() -> DataFrame:
    df = dp.read("bronze_bls_series")
    trimmed = [F.trim(F.col(c)).alias(c) for c in df.columns if df.schema[c].dataType.typeName() == "string"]
    other = [F.col(c) for c in df.columns if df.schema[c].dataType.typeName() != "string"]
    return df.select(*trimmed, *other)


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
@dp.expect_or_drop("year_present", "year IS NOT NULL")
@dp.expect_or_drop("population_present", "population IS NOT NULL")
def silver_population() -> DataFrame:
    df = dp.read("bronze_population")
    return df.select(
        F.trim(F.col("Nation_ID")).alias("nation_id"),
        F.trim(F.col("Nation")).alias("nation"),
        F.col("Year").cast("INT").alias("year"),
        F.col("Population").cast("BIGINT").alias("population"),
    )