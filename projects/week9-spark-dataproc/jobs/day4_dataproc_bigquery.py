# day4_dataproc_bigquery.py
# PySpark job for Dataproc Serverless — reads GCS, writes to BigQuery
# Week 9 - Day 4 | cloud-data-engineer-portfolio
#
# NOTE: No os.environ fixes needed here — Dataproc runs on Linux,
# no Windows compatibility issues. No local hadoop DLL problems.
# This is clean, production-grade PySpark.

import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, to_date, when, round as spark_round,
    month, year, datediff, current_date,
    sum as spark_sum, count, avg
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType
)

# ── CONFIG ──────────────────────────────────────────────────
# These are passed as command-line args when submitting the job
# so the same script works for any project/dataset without edits.
# sys.argv[1] = GCS input path
# sys.argv[2] = GCP project ID
# sys.argv[3] = BigQuery dataset name

if len(sys.argv) != 4:
    print("Usage: day4_dataproc_bigquery.py <gcs_input_path> <project_id> <bq_dataset>")
    sys.exit(1)

GCS_INPUT   = sys.argv[1]   # gs://your-bucket/data/orders_large.csv
PROJECT_ID  = sys.argv[2]   # your-project-id
BQ_DATASET  = sys.argv[3]   # week9_spark
BQ_TEMP_BUCKET = f"{PROJECT_ID}-spark-week9"  # GCS bucket for BQ temp files


# ── SPARK SESSION ────────────────────────────────────────────
# On Dataproc, .master() is NOT set — Dataproc injects the cluster
# master URL automatically. We just set the app name and BQ config.

spark = SparkSession.builder \
    .appName("Week9-Day4-Dataproc-BigQuery") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.dataproc.bigquery.temporaryGcsBucket", BQ_TEMP_BUCKET) \
    .config("temporaryGcsBucket", BQ_TEMP_BUCKET) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(f"Spark version: {spark.version}")
print(f"Reading from: {GCS_INPUT}")
print(f"Writing to BigQuery dataset: {PROJECT_ID}.{BQ_DATASET}")


# ── SCHEMA ───────────────────────────────────────────────────
ORDERS_SCHEMA = StructType([
    StructField("order_id",          IntegerType(), True),
    StructField("customer_id",       StringType(),  True),
    StructField("city",              StringType(),  True),
    StructField("product_category",  StringType(),  True),
    StructField("quantity",          IntegerType(), True),
    StructField("unit_price",        DoubleType(),  True),
    StructField("order_date",        StringType(),  True),
    StructField("status",            StringType(),  True),
])


# ── EXTRACT ──────────────────────────────────────────────────
print("\n[1/4] Extracting from GCS...")
raw_df = spark.read \
    .option("header", "true") \
    .schema(ORDERS_SCHEMA) \
    .csv(GCS_INPUT)

print(f"      Raw rows: {raw_df.count()}")


# ── CLEAN ────────────────────────────────────────────────────
print("\n[2/4] Cleaning...")
clean_df = raw_df \
    .filter(col("order_id").isNotNull()) \
    .fillna({"quantity": 0, "unit_price": 0.0, "status": "unknown"}) \
    .withColumn("city", upper(col("city"))) \
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
    .dropDuplicates(["order_id"])

print(f"      Clean rows: {clean_df.count()}")


# ── TRANSFORM ────────────────────────────────────────────────
print("\n[3/4] Transforming...")
transformed_df = clean_df \
    .withColumn(
        "total_value",
        spark_round(col("quantity") * col("unit_price"), 2)
    ) \
    .withColumn("order_month", month(col("order_date"))) \
    .withColumn("order_year",  year(col("order_date"))) \
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


# ── AGGREGATE ────────────────────────────────────────────────
completed_df = transformed_df.filter(col("status") == "completed")

city_report = completed_df \
    .groupBy("city", "product_category") \
    .agg(
        spark_round(spark_sum("total_value"), 2).alias("total_revenue"),
        count("order_id").alias("order_count"),
        spark_round(avg("total_value"), 2).alias("avg_order_value")
    ) \
    .orderBy("total_revenue", ascending=False)

monthly_report = completed_df \
    .groupBy("order_year", "order_month") \
    .agg(
        spark_round(spark_sum("total_value"), 2).alias("total_revenue"),
        count("order_id").alias("order_count")
    ) \
    .orderBy("order_year", "order_month")

tier_report = completed_df \
    .groupBy("value_tier") \
    .agg(
        count("order_id").alias("order_count"),
        spark_round(spark_sum("total_value"), 2).alias("total_revenue")
    ) \
    .orderBy("total_revenue", ascending=False)


# ── WRITE TO BIGQUERY ────────────────────────────────────────
# The Spark-BigQuery connector is pre-installed on Dataproc.
# .format("bigquery") tells Spark to use the connector.
# mode("overwrite") drops and recreates the BQ table.
# The connector first writes to a GCS temp location (temporaryGcsBucket),
# then loads from GCS into BigQuery in one atomic operation.

print("\n[4/4] Writing to BigQuery...")

def write_to_bq(df: DataFrame, table_name: str):
    full_table = f"{PROJECT_ID}.{BQ_DATASET}.{table_name}"
    print(f"      Writing → {full_table}")
    df.write \
        .format("bigquery") \
        .option("table", full_table) \
        .mode("overwrite") \
        .save()
    print(f"      ✓ Done: {full_table}")

write_to_bq(completed_df,   "orders_enriched")
write_to_bq(city_report,    "report_city_category")
write_to_bq(monthly_report, "report_monthly_trend")
write_to_bq(tier_report,    "report_value_tiers")

print("\n>> All 4 tables written to BigQuery successfully!")
print(f">> Check: https://console.cloud.google.com/bigquery?project={PROJECT_ID}")

spark.stop()
print("\n>> Day 4 complete!")