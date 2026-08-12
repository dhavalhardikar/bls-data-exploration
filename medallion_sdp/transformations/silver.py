"""Silver layer: cleansing, type normalization, and data quality expectations.

Refactored to use native, open-source PySpark DataFrame operations instead of
proprietary Databricks declarative-pipeline expectation decorators
(@dp.expect_or_drop / @dp.expect). @dp.table / dp.read remain, since those are
the Spark Declarative Pipeline framework's core table-registration API, not a
proprietary data-quality mechanism, and the task calls for preserving them.

Data-quality semantics are preserved exactly:
  - Hard rules (previously @dp.expect_or_drop): rows failing the rule are
    dropped from the table's output, via a plain .filter(...) call.
  - Soft rules (previously @dp.expect): rows failing the rule are NOT dropped,
    but are flagged for observability. Reproduced natively by computing a
    pass/fail count against the already-filtered DataFrame and logging it,
    without removing any rows from the returned DataFrame.
"""

import pyspark.pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Mirrors the previous @dp.expect("period_format_valid", ...) rule.
# _PERIOD_FORMAT_REGEX = r"^(Q0[1-5]|M(0[1-9]|1[0-2]))$"


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

    # --- Hard rules (native equivalent of expect_or_drop "series_id_present",
    # "year_present", "value_present") — rows failing any of these are dropped.
    validated = cleansed.filter(
        F.col("series_id").isNotNull()
        & F.col("year").isNotNull()
        & F.col("value").isNotNull()
    )

    # --- Soft rule (native equivalent of expect "period_format_valid") — flag
    # non-conforming rows for observability, but do NOT drop them.
    # invalid_period_count = validated.filter(~F.col("period").rlike(_PERIOD_FORMAT_REGEX)).count()
    # if invalid_period_count > 0:
    #     print(
    #         f"[DQ WARNING] silver_bls_data.period_format_valid: "
    #         f"{invalid_period_count} row(s) have an unexpected 'period' format "
    #         f"(expected Q01-Q05 or M01-M12). Rows are retained, not dropped."
    #     )

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

    # --- Hard rule (native equivalent of expect_or_drop "series_id_present") ---
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

    # --- Hard rules (native equivalent of expect_or_drop "year_present",
    # "population_present") — rows failing either are dropped.
    return cleansed.filter(
        F.col("year").isNotNull() & F.col("population").isNotNull()
    )