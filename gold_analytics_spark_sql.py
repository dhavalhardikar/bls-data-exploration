# Databricks notebook source
# MAGIC %md
# MAGIC # BLS Gold Analytics — Native Spark SQL (Platform-Agnostic)
# MAGIC
# MAGIC This notebook reproduces the original PySpark DataFrame-API gold-layer transformations using
# MAGIC **standard ANSI/Spark SQL**. It contains no Databricks-specific magics or utilities
# MAGIC (`%sql`, `%fs`, `%sh`, `dbutils`, `@dp`, `@dlt`, etc.) and will run unmodified on any
# MAGIC Spark-compliant engine that has Delta Lake configured — open-source Jupyter + PySpark/Spark SQL
# MAGIC kernel, AWS EMR, Google Dataproc, Spark on Kubernetes, or Databricks.
# MAGIC
# MAGIC **Pattern used throughout:**
# MAGIC 1. Read source Delta tables with `spark.read.table(...)` (or swap for `spark.read.format("delta").load(path)` if you are not using a Hive/Unity-style metastore).
# MAGIC 2. Register each DataFrame as a **temporary view** with `createOrReplaceTempView(...)`.
# MAGIC 3. Perform all business logic exclusively via `spark.sql("""...""")` calls.
# MAGIC 4. Persist Gold outputs with SQL DDL (`CREATE OR REPLACE TABLE ... USING DELTA AS SELECT ...`), the SQL equivalent of `.write.format("delta").mode("overwrite")`.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Session Initialization
# MAGIC
# MAGIC Standard `SparkSession` with Delta Lake support. No Databricks runtime is assumed.

# COMMAND ----------

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("BLS Gold Analytics - Native Spark SQL")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

    # NOTE: if running outside Databricks, launch with the delta-spark package, e.g.:
    #   pyspark --packages io.delta:delta-spark_2.12:3.2.0
)

spark.sql("SET spark.sql.ansi.enabled = true")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Silver Source Tables & Register Temp Views
# MAGIC
# MAGIC All downstream logic references these temp views only — no DataFrame API transformations
# MAGIC are used from this point forward.

# COMMAND ----------

spark.read.table("bls.bls_time_series.silver_bls_data").createOrReplaceTempView("silver_bls_data")
spark.read.table("bls.bls_time_series.silver_bls_measure").createOrReplaceTempView("silver_bls_measure")
spark.read.table("bls.bls_time_series.silver_bls_sector").createOrReplaceTempView("silver_bls_sector")
spark.read.table("bls.bls_time_series.silver_bls_series").createOrReplaceTempView("silver_bls_series")
spark.read.table("bls.bls_time_series.silver_population").createOrReplaceTempView("silver_population")


# COMMAND ----------

spark.sql("SELECT * FROM silver_bls_data LIMIT 20").show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create the Gold Target Schema
# MAGIC
# MAGIC Standard `CREATE SCHEMA IF NOT EXISTS` — no `%sql` magic required, issued through `spark.sql(...)`.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS bls.bls_pyspark_gold_tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold Query 1 — `gold_population_stats_2013_2018`
# MAGIC
# MAGIC Compute `AVG(population)` and `STDDEV(population)` for years 2013–2018 inclusive, entirely in SQL.

# COMMAND ----------

spark.sql("""
SELECT
    AVG(population)    AS mean_population,
    STDDEV(population) AS stddev_population
FROM silver_population
WHERE year BETWEEN 2013 AND 2018
""").createOrReplaceTempView("gold_population_stats_2013_2018")

spark.sql("SELECT * FROM gold_population_stats_2013_2018").show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Persist Gold Query 1 to Delta
# MAGIC
# MAGIC SQL DDL equivalent of `.write.format("delta").mode("overwrite").saveAsTable(...)`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bls.bls_spark_sql_gold_tables;

# COMMAND ----------

GOLD_POPULATION_STATS_PATH = "bls.bls_spark_sql_gold_tables.gold_population_stats"

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD_POPULATION_STATS_PATH}
USING DELTA
AS SELECT * FROM gold_population_stats_2013_2018
""")

print(f"Wrote gold_population_stats_2013_2018 -> {GOLD_POPULATION_STATS_PATH}")


# COMMAND ----------

spark.sql(f"SELECT * FROM {GOLD_POPULATION_STATS_PATH} LIMIT 10").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Gold Query 2 — `gold_best_year_per_series`
# MAGIC
# MAGIC Three SQL steps, matching the original DataFrame pipeline exactly:
# MAGIC
# MAGIC 1. **Quarterly totals** — filter `period IN ('Q01','Q02','Q03','Q04')`, sum `value` per `(series_id, year)`, rounded to 2 decimals.
# MAGIC 2. **Best year** — `ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY total_value DESC)`, keep rank 1.
# MAGIC 3. **Series labels** — left join series → sector → measure metadata to build a human-readable `series_title`, then join onto `best_year`.

# COMMAND ----------

spark.sql("""
SELECT
    series_id,
    year,
    ROUND(SUM(value), 2) AS total_value
FROM silver_bls_data
WHERE period IN ('Q01', 'Q02', 'Q03', 'Q04')
GROUP BY series_id, year
""").createOrReplaceTempView("quarterly_totals")

spark.sql("SELECT * FROM quarterly_totals").show(20, truncate=False)

# COMMAND ----------

spark.sql("""
SELECT series_id, year, total_value
FROM (
    SELECT
        series_id,
        year,
        total_value,
        ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY total_value DESC) AS rnk
    FROM quarterly_totals
)
WHERE rnk = 1
""").createOrReplaceTempView("best_year")

spark.sql("SELECT * FROM best_year").show(20, truncate=False)


# COMMAND ----------

spark.sql("DESCRIBE silver_bls_series").show(truncate=False)
spark.sql("DESCRIBE silver_bls_sector").show(truncate=False)
spark.sql("DESCRIBE silver_bls_measure").show(truncate=False)


# COMMAND ----------

spark.sql("""
SELECT
    s.series_id,
    m.measure_text                                   AS measure,
    sec.sector_name                                   AS industry,
    CONCAT_WS(' - ', sec.sector_name, m.measure_text)  AS series_title
FROM silver_bls_series s
LEFT JOIN silver_bls_sector  sec ON s.sector_code  = sec.sector_code
LEFT JOIN silver_bls_measure m   ON s.measure_code = m.measure_code
""").createOrReplaceTempView("series_with_labels")

spark.sql("SELECT * FROM series_with_labels").show(20, truncate=False)


# COMMAND ----------

spark.sql("""
SELECT
    b.series_id,
    b.year,
    b.total_value,
    l.measure,
    l.industry,
    l.series_title
FROM best_year b
LEFT JOIN series_with_labels l ON b.series_id = l.series_id
""").createOrReplaceTempView("gold_best_year_per_series")

spark.sql("SELECT * FROM gold_best_year_per_series").show(20, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Persist Gold Query 2 to Delta

# COMMAND ----------

GOLD_BEST_YEAR_PER_SERIES_PATH = "bls.bls_spark_sql_gold_tables.gold_best_year_per_series"

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD_BEST_YEAR_PER_SERIES_PATH}
USING DELTA
AS SELECT * FROM gold_best_year_per_series
""")

print(f"Wrote gold_best_year_per_series -> {GOLD_BEST_YEAR_PER_SERIES_PATH}")


# COMMAND ----------

spark.sql(f"SELECT * FROM {GOLD_BEST_YEAR_PER_SERIES_PATH} LIMIT 10").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Gold Query 3 — `gold_prs30006032_q1_population`
# MAGIC
# MAGIC Filter `silver_bls_data` down to a single target series (`PRS30006032`) and `period = 'Q01'`,
# MAGIC then `LEFT JOIN` against `silver_population` on `year` to append population figures.
# MAGIC
# MAGIC > Note: the original notebook referenced an undefined variable `silver_bls_data` (missing the
# MAGIC > `_df` suffix / temp-view registration) at this step. Since we now query exclusively through
# MAGIC > the `silver_bls_data` temp view registered in Section 2, this reference resolves correctly and
# MAGIC > the underlying business logic (series + period filter, left join on year) is unchanged.

# COMMAND ----------

TARGET_SERIES_ID = "PRS30006032"

spark.sql(f"""
SELECT *
FROM silver_bls_data
WHERE series_id = '{TARGET_SERIES_ID}'
  AND period = 'Q01'
""").createOrReplaceTempView("series_q1")

spark.sql("SELECT * FROM series_q1").show(20, truncate=False)

# COMMAND ----------

spark.sql("""
SELECT
    sq.*,
    p.population
FROM series_q1 sq
LEFT JOIN (SELECT year, population FROM silver_population) p
    ON sq.year = p.year
""").createOrReplaceTempView("gold_prs30006032_q1_population")

spark.sql("SELECT * FROM gold_prs30006032_q1_population").show(20, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Persist Gold Query 3 to Delta

# COMMAND ----------

GOLD_PRS30006032_Q1_POPULATION_PATH = "bls.bls_spark_sql_gold_tables.gold_prs30006032_q1_population"

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD_PRS30006032_Q1_POPULATION_PATH}
USING DELTA
AS SELECT * FROM gold_prs30006032_q1_population
""")

print(f"Wrote gold_prs30006032_q1_population -> {GOLD_PRS30006032_Q1_POPULATION_PATH}")


# COMMAND ----------

spark.sql(f"SELECT * FROM {GOLD_PRS30006032_Q1_POPULATION_PATH} LIMIT 10").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Gold Table | Source Logic | Persistence |
# MAGIC |---|---|---|
# MAGIC | `gold_population_stats` | `AVG`/`STDDEV(population)` for `year BETWEEN 2013 AND 2018` | `CREATE OR REPLACE TABLE ... USING DELTA` |
# MAGIC | `gold_best_year_per_series` | Quarterly `SUM(value)` → `ROW_NUMBER()` best year → label join | `CREATE OR REPLACE TABLE ... USING DELTA` |
# MAGIC | `gold_prs30006032_q1_population` | `series_id`/`period='Q01'` filter → `LEFT JOIN` on `year` | `CREATE OR REPLACE TABLE ... USING DELTA` |
# MAGIC
# MAGIC All transformation logic (filters, aggregation formulas, join conditions, window function
# MAGIC semantics) is identical to the original PySpark DataFrame-API notebook — only the execution
# MAGIC API changed, from DataFrame method chains to native Spark SQL executed via `spark.sql(...)`.