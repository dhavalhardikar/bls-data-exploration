# Databricks notebook source
from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName("Pyspark Gold table Re-write").getOrCreate()

silver_bls_data_df = spark.read.table("bls.bls_time_series.silver_bls_data")
silver_bls_measure_df = spark.read.table("bls.bls_time_series.silver_bls_measure")
silver_bls_sector_df = spark.read.table("bls.bls_time_series.silver_bls_sector")
silver_bls_series_df = spark.read.table("bls.bls_time_series.silver_bls_series")

# COMMAND ----------

silver_bls_data_df.show(20, truncate=False)

# COMMAND ----------

silver_population_df = spark.read.table("bls.bls_time_series.silver_population")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

gold_population_stats_2013_2018 = silver_population_df \
    .filter(F.col("year").between(2013, 2018)) \
    .agg( \
    F.mean("population").alias("mean_population"), \
    F.stddev("population").alias("stddev_population"),\
)

# COMMAND ----------

gold_population_stats_2013_2018.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bls.bls_pyspark_gold_tables;

# COMMAND ----------

GOLD_POPULATION_STATS_PATH = "bls.bls_pyspark_gold_tables.gold_population_stats"

gold_population_stats_2013_2018.write.format("delta").mode("overwrite").saveAsTable(GOLD_POPULATION_STATS_PATH)

print(f"Wrote gold_population_stats_2013_2018 -> {GOLD_POPULATION_STATS_PATH}")

# COMMAND ----------

QUARTER_PERIODS = ["Q01", "Q02", "Q03", "Q04"]

quarterly_totals = silver_bls_data_df \
    .filter(F.col("period").isin(QUARTER_PERIODS)) \
    .groupBy("series_id", "year") \
    .agg(F.round(F.sum("value"),2).alias("total_value"))

window = Window.partitionBy("series_id").orderBy(F.col("total_value").desc())

best_year = (
    quarterly_totals
    .withColumn("rank", F.row_number().over(window))
    .filter(F.col("rank") == 1)
    .drop("rank")
)


# COMMAND ----------

best_year.show(20, truncate=False)

# COMMAND ----------

silver_bls_series_df.printSchema()

# COMMAND ----------

silver_bls_sector_df.printSchema()

# COMMAND ----------

silver_bls_measure_df.printSchema()

# COMMAND ----------


series_with_labels = (
        silver_bls_series_df.join(silver_bls_sector_df, on="sector_code", how="left")
        .join(silver_bls_measure_df, on="measure_code", how="left")
        .select(
            "series_id",
            F.col("measure_text").alias("measure"),
            F.col("sector_name").alias("industry"),
            F.concat_ws(" - ", F.col("sector_name"), F.col("measure_text")).alias("series_title"),
        )
    )

# COMMAND ----------

series_with_labels.show(20, truncate=False)

# COMMAND ----------

gold_best_year_per_series = best_year.join(series_with_labels, on="series_id", how="left")

# COMMAND ----------

gold_best_year_per_series.show(20, truncate=False)

# COMMAND ----------

GOLD_BEST_YEAR_PER_SERIES_PATH = "bls.bls_pyspark_gold_tables.gold_best_year_per_series"

gold_best_year_per_series.write.format("delta").mode("overwrite").saveAsTable(GOLD_BEST_YEAR_PER_SERIES_PATH)


# COMMAND ----------

TARGET_SERIES_ID = "PRS30006032"

series_q1 = silver_bls_data.filter(
    (F.col("series_id") == TARGET_SERIES_ID) & (F.col("period") == "Q01")
)

gold_prs30006032_q1_population = series_q1.join(
    silver_population_df.select("year", "population"),
    on="year",
    how="left",
)

# COMMAND ----------

gold_prs30006032_q1_population.show(20, truncate=False)

# COMMAND ----------

GOLD_PRS30006032_Q1_POPULATION_PATH = "bls.bls_pyspark_gold_tables.gold_prs30006032_q1_population"

gold_prs30006032_q1_population.write.format("delta").mode("overwrite").saveAsTable(GOLD_PRS30006032_Q1_POPULATION_PATH)
