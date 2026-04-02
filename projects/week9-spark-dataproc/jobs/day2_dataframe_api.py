# day2_dataframe_api.py
# Purpose: Master the DataFrame API — select, filter, withColumn, join, agg, write Parquet
# Week 9 - Day 2 | cloud-data-engineer-portfolio

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, round as spark_round,
    when, upper, to_date, datediff, current_date,
    avg, max as spark_max, min as spark_min,
    coalesce, lit, concat, month, year
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, DateType
)

# ─────────────────────────────────────────────
# 1. SPARK SESSION
# ─────────────────────────────────────────────
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Week9-Day2-DataFrame-API") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("Day 2 — DataFrame API Mastery")
print("=" * 60)

# ─────────────────────────────────────────────
# 2. DEFINE SCHEMA MANUALLY (Production Practice)
# ─────────────────────────────────────────────
orders_schema = StructType([
    StructField("order_id", IntegerType(), nullable=True),
    StructField("customer_id", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("product_category", StringType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=True),
    StructField("unit_price", DoubleType(), nullable=True),
    StructField("order_date", StringType(), nullable=True),
    StructField("status", StringType(), nullable=True),
])

orders_df = spark.read \
    .option("header", "true") \
    .schema(orders_schema) \
    .csv("data/orders.csv")

print("\n>> Orders loaded with manual schema:")
orders_df.printSchema()

# ─────────────────────────────────────────────
# 3. CREATE CUSTOMERS TABLE
# ─────────────────────────────────────────────
customers_data = [
    ("C001", "Aisha Khan", "Premium", "2023-06-01"),
    ("C002", "Rohan Mehta", "Standard", "2023-08-15"),
    ("C003", "Priya Nair", "Premium", "2023-07-20"),
    ("C004", "Arjun Patel", "Standard", "2023-09-10"),
    ("C005", "Sneha Reddy", "Premium", "2023-05-30"),
    ("C006", "Vikram Singh", "Standard", "2023-10-01"),
    ("C007", "Divya Sharma", "Premium", "2023-11-15"),
    ("C008", "Karan Joshi", "Standard", "2023-12-20"),
    ("C009", "Meera Iyer", "Premium", "2024-01-05"),
    ("C010", "Rahul Gupta", "Standard", "2024-01-10"),
]

customers_df = spark.createDataFrame(
    customers_data,
    ["customer_id", "customer_name", "membership_tier", "joined_date"]
)

print("\n>> Customers table created:")
customers_df.show(truncate=False)

# ─────────────────────────────────────────────
# 4. DATA CLEANING
# ─────────────────────────────────────────────
print("\n>> Null counts per column:")
from pyspark.sql.functions import isnan, isnull

null_counts = orders_df.select([
    count(when(isnull(c), c)).alias(c)
    for c in orders_df.columns
])
null_counts.show()

orders_clean = orders_df.fillna({
    "quantity": 0,
    "unit_price": 0.0,
    "status": "unknown"
})

orders_clean = orders_clean.withColumn("city", upper(col("city")))

orders_clean = orders_clean.withColumn(
    "order_date",
    to_date(col("order_date"), "yyyy-MM-dd")
)

orders_clean = orders_clean.dropDuplicates(["order_id"])

print("\n>> Cleaned orders (city uppercased, date converted):")
orders_clean.select("order_id", "city", "order_date", "status").show(5)

# ─────────────────────────────────────────────
# 5. FEATURE ENGINEERING
# ─────────────────────────────────────────────
orders_enriched = orders_clean.withColumn(
    "total_value",
    spark_round(col("quantity") * col("unit_price"), 2)
)

orders_enriched = orders_enriched \
    .withColumn("order_month", month(col("order_date"))) \
    .withColumn("order_year", year(col("order_date")))

orders_enriched = orders_enriched.withColumn(
    "is_high_value",
    when(col("total_value") > 10000, "YES").otherwise("NO")
)

orders_enriched = orders_enriched.withColumn(
    "days_since_order",
    datediff(current_date(), col("order_date"))
)

print("\n>> Enriched orders with calculated columns:")
orders_enriched.select(
    "order_id", "total_value", "is_high_value",
    "order_month", "days_since_order"
).show(5)

# ─────────────────────────────────────────────
# 6. JOIN — Orders + Customers
# ─────────────────────────────────────────────
joined_df = orders_enriched.join(
    customers_df,
    on="customer_id",
    how="left"
)

joined_df = joined_df.withColumn(
    "customer_name",
    coalesce(col("customer_name"), lit("Unknown"))
)

print("\n>> Joined orders + customers:")
joined_df.select(
    "order_id", "customer_name", "membership_tier",
    "city", "total_value", "is_high_value"
).show(10, truncate=False)

# ─────────────────────────────────────────────
# 7. AGGREGATIONS
# ─────────────────────────────────────────────
print("\n>> Report 1: Revenue by City + Membership Tier:")
report1 = joined_df \
    .filter(col("status") == "completed") \
    .groupBy("city", "membership_tier") \
    .agg(
        spark_round(spark_sum("total_value"), 2).alias("total_revenue"),
        count("order_id").alias("order_count"),
        spark_round(avg("total_value"), 2).alias("avg_order_value"),
        spark_round(spark_max("total_value"), 2).alias("max_order_value")
    ) \
    .orderBy("total_revenue", ascending=False)

report1.show(truncate=False)

print("\n>> Report 2: High Value vs Normal Orders:")
report2 = joined_df \
    .filter(col("status") == "completed") \
    .groupBy("is_high_value") \
    .agg(
        count("order_id").alias("order_count"),
        spark_round(spark_sum("total_value"), 2).alias("total_revenue"),
        spark_round(avg("total_value"), 2).alias("avg_value")
    )

report2.show(truncate=False)

print("\n>> Report 3: Top Customers by Revenue:")
report3 = joined_df \
    .filter(col("status") == "completed") \
    .groupBy("customer_id", "customer_name", "membership_tier") \
    .agg(
        spark_round(spark_sum("total_value"), 2).alias("total_spent"),
        count("order_id").alias("total_orders")
    ) \
    .orderBy("total_spent", ascending=False)

report3.show(truncate=False)

# ─────────────────────────────────────────────
# 8. WRITE OUTPUT
# ─────────────────────────────────────────────
# NOTE: Writing Parquet to local disk on Windows requires Hadoop native
# libraries that have version conflicts with PySpark 3.5.1.
# For local development we write CSV (no native IO needed).
# On Dataproc (Day 4), we write Parquet directly to GCS — no issues.
# This is actually how real pipelines work anyway.

import pathlib
pathlib.Path("output").mkdir(exist_ok=True)

print("\n>> Writing outputs as CSV (local dev mode)...")

joined_df \
    .filter(col("status") == "completed") \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("output/orders_enriched_csv")

report1.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("output/report_city_tier_csv")

report3.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("output/report_customers_csv")

print(">> CSV files written to output/ folder")

# ─────────────────────────────────────────────
# 9. READ BACK AND VERIFY
# ─────────────────────────────────────────────
print("\n>> Reading back CSV to verify:")
verify_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("output/orders_enriched_csv")
print(f"   Row count from CSV: {verify_df.count()}")
verify_df.show(5, truncate=False)

spark.stop()
print("\n>> Day 2 complete!")



# ─────────────────────────────────────────────
# 10. SPARK UI
# ─────────────────────────────────────────────
print("\n" + "=" * 60)
print(">> SPARK UI IS LIVE at http://localhost:4040")
print(">> Open your browser NOW — you have 60 seconds")
print(">> Look for: Jobs tab, Stages tab, SQL tab")
print("=" * 60)
#time.sleep(60)

spark.stop()
print("\n>> Day 2 complete!")