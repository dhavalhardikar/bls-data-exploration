"""Gold layer: analytical deliverables built on top of Silver."""

import pyspark.pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# series_id -> (sector_code, class_code, measure_code, duration_code, ...) lives in
# silver_bls_series; sector_code and measure_code are then resolved to human-readable
# labels via silver_bls_sector / silver_bls_measure. pr.series has no single title
# field (unlike e.g. cu.series), so series_title is synthesized from the two labels.
QUARTER_PERIODS = ["Q01", "Q02", "Q03", "Q04"]
TARGET_SERIES_ID = "PRS30006032"


@dp.table(
    name="gold_population_stats_2013_2018",
    comment="Mean and standard deviation of US population, 2013-2018 inclusive.",
)
def gold_population_stats_2013_2018() -> DataFrame:
    df = dp.read("silver_population")
    return df.filter(F.col("year").between(2013, 2018)).agg(
        F.mean("population").alias("mean_population"),
        F.stddev("population").alias("stddev_population"),
    )


@dp.table(
    name="gold_best_year_per_series",
    comment="Best (highest total quarterly value) year per BLS series, with human-readable series metadata.",
)
def gold_best_year_per_series() -> DataFrame:
    data = dp.read("silver_bls_data")
    series = dp.read("silver_bls_series")
    sector = dp.read("silver_bls_sector")
    measure = dp.read("silver_bls_measure")

    quarterly_totals = (
        data.filter(F.col("period").isin(QUARTER_PERIODS))
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
        series.join(sector, on="sector_code", how="left")
        .join(measure, on="measure_code", how="left")
        .select(
            "series_id",
            F.col("measure_text").alias("measure"),
            F.col("sector_name").alias("industry"),
            F.concat_ws(" - ", F.col("sector_name"), F.col("measure_text")).alias("series_title"),
        )
    )

    return best_year.join(series_with_labels, on="series_id", how="left")


@dp.table(
    name="gold_prs30006032_q1_population",
    comment=f"Q1 values for series {TARGET_SERIES_ID} alongside that year's national population.",
)
def gold_prs30006032_q1_population() -> DataFrame:
    data = dp.read("silver_bls_data")
    population = dp.read("silver_population")

    series_q1 = data.filter((F.col("series_id") == TARGET_SERIES_ID) & (F.col("period") == "Q01"))

    return series_q1.join(
        population.select("year", "population"),
        on="year",
        how="left",
    )
