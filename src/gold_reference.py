"""Standalone PySpark reference implementations of the Gold layer.

These mirror pipelines/gold.py's transformation logic exactly, but take plain
DataFrames as arguments and return a plain DataFrame — no `import pyspark.pipelines
as dp`, no `@dp.table`, no dependency on a running declarative pipeline. That makes
them usable:
  - in a regular notebook cell for ad-hoc validation against Silver tables,
  - in a pytest suite with a local SparkSession and small synthetic DataFrames,
  - to sanity-check pipelines/gold.py's output by diffing against these.

Keep this in sync with pipelines/gold.py when the Gold logic changes — or better,
have gold.py import and call these directly (see the note at the bottom of that
file).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# pr.series has no title field of its own — series_id maps to sector_code/measure_code,
# which are resolved to labels via pr.sector / pr.measure, then combined into a title.
QUARTER_PERIODS = ["Q01", "Q02", "Q03", "Q04"]
TARGET_SERIES_ID = "PRS30006032"


def population_stats_2013_2018(silver_population: DataFrame) -> DataFrame:
    """Mean and standard deviation of population, 2013-2018 inclusive."""
    return silver_population.filter(F.col("year").between(2013, 2018)).agg(
        F.mean("population").alias("mean_population"),
        F.stddev("population").alias("stddev_population"),
    )


def best_year_per_series(
    silver_bls_data: DataFrame,
    silver_bls_series: DataFrame,
    silver_bls_sector: DataFrame,
    silver_bls_measure: DataFrame,
) -> DataFrame:
    """Highest-total-quarterly-value year per series, joined with human-readable series metadata."""
    quarterly_totals = (
        silver_bls_data.filter(F.col("period").isin(QUARTER_PERIODS))
        .groupBy("series_id", "year")
        .agg(F.sum("value").alias("total_value"))
    )

    window = Window.partitionBy("series_id").orderBy(F.col("total_value").desc())
    best_year = (
        quarterly_totals.withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") == 1)
        .drop("rank")
    )

    series_with_labels = (
        silver_bls_series.join(silver_bls_sector, on="sector_code", how="left")
        .join(silver_bls_measure, on="measure_code", how="left")
        .select(
            "series_id",
            F.col("measure_text").alias("measure"),
            F.col("sector_name").alias("industry"),
            F.concat_ws(" - ", F.col("sector_name"), F.col("measure_text")).alias("series_title"),
        )
    )

    return best_year.join(series_with_labels, on="series_id", how="left")


def prs30006032_q1_population(silver_bls_data: DataFrame, silver_population: DataFrame) -> DataFrame:
    """Q1 values for series PRS30006032 left-joined with that year's national population."""
    series_q1 = silver_bls_data.filter(
        (F.col("series_id") == TARGET_SERIES_ID) & (F.col("period") == "Q01")
    )
    return series_q1.join(
        silver_population.select("year", "population"),
        on="year",
        how="left",
    )
