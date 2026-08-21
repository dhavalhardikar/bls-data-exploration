"""Gold layer (Spark SQL variant): analytical deliverables built on top of Silver.

This is an ALTERNATIVE implementation of the same 3 Gold queries defined in
gold.py, expressed with native Spark SQL (CREATE OR REPLACE TEMP VIEW +
spark.sql(...)) instead of the PySpark DataFrame API.
"""

import pyspark.pipelines as dp
from pyspark.sql import DataFrame

QUARTER_PERIODS_SQL = "('Q01', 'Q02', 'Q03', 'Q04')"
TARGET_SERIES_ID = "PRS30006032"


@dp.table(
    name="gold_population_stats_2013_2018_sql",
    comment="[Spark SQL variant] Mean and standard deviation of US population, 2013-2018 inclusive.",
)
def gold_population_stats_2013_2018_sql() -> DataFrame:
    dp.read("silver_population").createOrReplaceTempView("silver_population_v")

    return spark.sql(
        """
        SELECT
            MEAN(population)   AS mean_population,
            STDDEV(population) AS stddev_population
        FROM silver_population_v
        WHERE year BETWEEN 2013 AND 2018
        """
    )


@dp.table(
    name="gold_best_year_per_series_sql",
    comment="[Spark SQL variant] Best (highest total quarterly value) year per BLS series, with human-readable series metadata.",
)
def gold_best_year_per_series_sql() -> DataFrame:
    dp.read("silver_bls_data").createOrReplaceTempView("silver_bls_data_v")
    dp.read("silver_bls_series").createOrReplaceTempView("silver_bls_series_v")
    dp.read("silver_bls_sector").createOrReplaceTempView("silver_bls_sector_v")
    dp.read("silver_bls_measure").createOrReplaceTempView("silver_bls_measure_v")

    # Step 1: total quarterly value per series/year
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW quarterly_totals_v AS
        SELECT
            series_id,
            year,
            SUM(value) AS total_value
        FROM silver_bls_data_v
        WHERE period IN {QUARTER_PERIODS_SQL}
        GROUP BY series_id, year
        """
    )

    # Step 2: rank years per series by total_value, keep only rank 1 (best year)
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW best_year_v AS
        SELECT series_id, year, total_value
        FROM (
            SELECT
                series_id,
                year,
                total_value,
                ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY total_value DESC) AS rnk
            FROM quarterly_totals_v
        )
        WHERE rnk = 1
        """
    )

    # Step 3: resolve series_id -> human-readable industry/measure/title
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW series_with_labels_v AS
        SELECT
            s.series_id,
            m.measure_text                                        AS measure,
            sec.sector_name                                       AS industry,
            CONCAT_WS(' - ', sec.sector_name, m.measure_text)      AS series_title
        FROM silver_bls_series_v s
        LEFT JOIN silver_bls_sector_v  sec ON s.sector_code  = sec.sector_code
        LEFT JOIN silver_bls_measure_v m   ON s.measure_code = m.measure_code
        """
    )

    # Step 4: join best year per series with its human-readable labels
    return spark.sql(
        """
        SELECT
            b.series_id,
            b.year,
            b.total_value,
            l.measure,
            l.industry,
            l.series_title
        FROM best_year_v b
        LEFT JOIN series_with_labels_v l ON b.series_id = l.series_id
        """
    )


@dp.table(
    name="gold_prs30006032_q1_population_sql",
    comment=f"[Spark SQL variant] Q1 values for series {TARGET_SERIES_ID} alongside that year's national population.",
)
def gold_prs30006032_q1_population_sql() -> DataFrame:
    dp.read("silver_bls_data").createOrReplaceTempView("silver_bls_data_v")
    dp.read("silver_population").createOrReplaceTempView("silver_population_v")

    return spark.sql(
        f"""
        SELECT
            d.*,
            p.population
        FROM (
            SELECT *
            FROM silver_bls_data_v
            WHERE series_id = '{TARGET_SERIES_ID}' AND period = 'Q01'
        ) d
        LEFT JOIN silver_population_v p
            ON d.year = p.year
        """
    )
