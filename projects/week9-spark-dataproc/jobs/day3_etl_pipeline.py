# day3_etl_pipeline.py
# Production-grade ETL broken into reusable functions
# Week 9 - Day 3 | cloud-data-engineer-portfolio

import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, to_date, when, round as spark_round,
    month, year, datediff, current_date, sum as spark_sum,
    count, avg, lit, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType
)


# ─────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────
def create_spark_session(app_name: str = "Week9-Day3-ETL") -> SparkSession:
    """Create and return a configured SparkSession."""
    return SparkSession.builder \
        .master("local[*]") \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()


# ─────────────────────────────────────────────
# SCHEMA
# ─────────────────────────────────────────────
ORDERS_SCHEMA = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
])


# ─────────────────────────────────────────────
# STEP 1: EXTRACT
# ─────────────────────────────────────────────
def extract_orders(spark: SparkSession, path: str) -> DataFrame:
    """
    Read raw orders CSV with predefined schema.
    Using manual schema avoids the inferSchema scan pass.
    """
    return spark.read \
        .option("header", "true") \
        .schema(ORDERS_SCHEMA) \
        .csv(path)


# ─────────────────────────────────────────────
# STEP 2: CLEAN
# ─────────────────────────────────────────────
def clean_orders(df: DataFrame) -> DataFrame:
    """
    Clean raw orders:
    - Standardize city to uppercase
    - Convert order_date string to date type
    - Fill nulls with safe defaults
    - Remove duplicates on order_id
    - Filter out rows with null order_id (corrupt records)
    """
    return df \
        .filter(col("order_id").isNotNull()) \
        .fillna({"quantity": 0, "unit_price": 0.0, "status": "unknown"}) \
        .withColumn("city", upper(col("city"))) \
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
        .dropDuplicates(["order_id"])


# ─────────────────────────────────────────────
# STEP 3: TRANSFORM
# ─────────────────────────────────────────────
def transform_orders(df: DataFrame) -> DataFrame:
    """
    Add business columns:
    - total_value: revenue per order line
    - order_month, order_year: for time-based reporting
    - is_high_value: flag orders > 10000
    - value_tier: Bronze / Silver / Gold segmentation
    - days_since_order: recency metric
    """
    return df \
        .withColumn(
            "total_value",
            spark_round(col("quantity") * col("unit_price"), 2)
        ) \
        .withColumn("order_month", month(col("order_date"))) \
        .withColumn("order_year", year(col("order_date"))) \
        .withColumn(
            "is_high_value",
            when(col("total_value") > 10000, "YES").otherwise("NO")
        ) \
        .withColumn(
            "value_tier",
            when(col("total_value") >= 50000, "Gold")
            .when(col("total_value") >= 10000, "Silver")
            .otherwise("Bronze")
        ) \
        .withColumn(
            "days_since_order",
            datediff(current_date(), col("order_date"))
        )


# ─────────────────────────────────────────────
# STEP 4: AGGREGATE
# ─────────────────────────────────────────────
def build_city_report(df: DataFrame) -> DataFrame:
    """Aggregate completed orders by city."""
    return df \
        .filter(col("status") == "completed") \
        .groupBy("city", "product_category") \
        .agg(
            spark_round(spark_sum("total_value"), 2).alias("total_revenue"),
            count("order_id").alias("order_count"),
            spark_round(avg("total_value"), 2).alias("avg_order_value")
        ) \
        .orderBy("total_revenue", ascending=False)


def build_monthly_report(df: DataFrame) -> DataFrame:
    """Aggregate completed orders by month."""
    return df \
        .filter(col("status") == "completed") \
        .groupBy("order_year", "order_month") \
        .agg(
            spark_round(spark_sum("total_value"), 2).alias("total_revenue"),
            count("order_id").alias("order_count"),
            spark_round(avg("total_value"), 2).alias("avg_order_value")
        ) \
        .orderBy("order_year", "order_month")


def build_tier_report(df: DataFrame) -> DataFrame:
    """Aggregate orders by value tier."""
    return df \
        .filter(col("status") == "completed") \
        .groupBy("value_tier") \
        .agg(
            count("order_id").alias("order_count"),
            spark_round(spark_sum("total_value"), 2).alias("total_revenue")
        ) \
        .orderBy("total_revenue", ascending=False)


# ─────────────────────────────────────────────
# STEP 5: PARTITION MANAGEMENT + WRITE
# ─────────────────────────────────────────────
def show_partition_info(df: DataFrame, label: str) -> None:
    """Print partition count and size distribution."""
    num_partitions = df.rdd.getNumPartitions()
    print(f"\n>> [{label}] Number of partitions: {num_partitions}")


def write_csv_output(df: DataFrame, path: str, num_partitions: int = 1) -> None:
    """
    Write DataFrame to CSV after coalescing to control file count.
    coalesce(1) = single output file (good for reports)
    coalesce(4) = 4 files (good for large datasets with parallel reads)

    WHY coalesce not repartition?
    - We're REDUCING partitions (always use coalesce for reduction)
    - coalesce avoids a shuffle — just merges partition data locally
    - repartition would cause an unnecessary full shuffle here
    """
    df.coalesce(num_partitions) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(path)


# ─────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────
def run_pipeline():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("Day 3 — Production ETL Pipeline")
    print("=" * 60)

    # ── EXTRACT ──
    print("\n[1/5] Extracting data...")
    raw_df = extract_orders(spark, "data/orders_large.csv")
    print(f"      Raw row count: {raw_df.count()}")
    show_partition_info(raw_df, "After extract")

    # ── CLEAN ──
    print("\n[2/5] Cleaning data...")
    clean_df = clean_orders(raw_df)
    print(f"      Clean row count: {clean_df.count()}")
    show_partition_info(clean_df, "After clean")

    # ── TRANSFORM ──
    print("\n[3/5] Transforming data...")
    transformed_df = transform_orders(clean_df)
    transformed_df.select(
        "order_id", "city", "total_value",
        "value_tier", "is_high_value", "days_since_order"
    ).show(10, truncate=False)

    # ── DEMONSTRATE REPARTITION vs COALESCE ──
    print("\n[4/5] Partition management demo...")

    show_partition_info(transformed_df, "Before any change")

    # repartition — full shuffle, evenly balanced
    repartitioned = transformed_df.repartition(16)
    show_partition_info(repartitioned, "After repartition(16)")

    # coalesce — no shuffle, just merging
    coalesced = transformed_df.coalesce(2)
    show_partition_info(coalesced, "After coalesce(2)")

    print("""
    KEY INSIGHT:
    - repartition(16): triggered a full shuffle, data evenly spread
    - coalesce(2):     NO shuffle, just merged existing partitions
    - For writing output files, always coalesce BEFORE write
    - Rule: reduce partitions = coalesce, increase = repartition
    """)

    # ── AGGREGATE + WRITE ──
    print("\n[5/5] Building reports...")

    city_report = build_city_report(transformed_df)
    monthly_report = build_monthly_report(transformed_df)
    tier_report = build_tier_report(transformed_df)

    print("\n>> City + Category Revenue (top 10):")
    city_report.show(10, truncate=False)

    print("\n>> Monthly Revenue Trend:")
    monthly_report.show(truncate=False)

    print("\n>> Value Tier Breakdown:")
    tier_report.show(truncate=False)

    # Write with controlled partition count
    import pathlib
    pathlib.Path("output").mkdir(exist_ok=True)

    write_csv_output(transformed_df.filter(col("status") == "completed"),
                     "output/day3_orders_enriched", num_partitions=1)
    write_csv_output(city_report, "output/day3_city_report", num_partitions=1)
    write_csv_output(monthly_report, "output/day3_monthly_report", num_partitions=1)

    print("\n>> All outputs written to output/")

    # Verify
    verify = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("output/day3_orders_enriched")
    print(f"\n>> Verified enriched output: {verify.count()} completed orders")

    spark.stop()
    print("\n>> Day 3 Pipeline complete!")


if __name__ == "__main__":
    run_pipeline()