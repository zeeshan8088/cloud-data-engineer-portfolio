# day1_intro.py
# Purpose: First PySpark job — understand lazy evaluation, transformations, actions
# Week 9 - Day 1 | cloud-data-engineer-portfolio

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, round as spark_round

# ─────────────────────────────────────────────
# 1. CREATE SPARK SESSION
# ─────────────────────────────────────────────
# SparkSession is the entry point to everything in Spark.
# Think of it as "opening the office" before the manager and workers can start.
# .master("local[*]") means: run locally, use ALL available CPU cores as executors
# .appName() is just a label that shows up in the Spark UI

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Week9-Day1-Intro") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Reduce log noise — only show warnings and errors, not every INFO message
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("Spark Session started successfully")
print(f"Spark version: {spark.version}")
print("=" * 60)

# ─────────────────────────────────────────────
# 2. READ DATA — This is a TRANSFORMATION (lazy)
# ─────────────────────────────────────────────
# Nothing actually happens here yet. Spark just notes:
# "okay, when I need to, I'll read this CSV with a header row
# and figure out the column types automatically (inferSchema)"

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/orders.csv")

print("\n>> Schema of the DataFrame (what Spark inferred):")
df.printSchema()
# printSchema() is an ACTION — this triggers Spark to actually read the file

# ─────────────────────────────────────────────
# 3. TRANSFORMATIONS — Building the plan (lazy)
# ─────────────────────────────────────────────
# None of these lines execute yet. Spark is just building the DAG.

# Filter: keep only completed orders
completed_orders = df.filter(col("status") == "completed")

# Add a new column: total_value = quantity * unit_price
with_total = completed_orders.withColumn(
    "total_value",
    spark_round(col("quantity") * col("unit_price"), 2)
)

# Select only the columns we care about
selected = with_total.select(
    "order_id", "city", "product_category", "total_value", "order_date"
)

print("\n>> Transformations defined. Nothing has executed yet.")
print("   (filter, withColumn, select are all lazy)")

# ─────────────────────────────────────────────
# 4. ACTIONS — These trigger actual execution
# ─────────────────────────────────────────────

# ACTION 1: show() — triggers Spark to execute all transformations above
print("\n>> ACTION: show() — Spark executes now:")
selected.show(10, truncate=False)

# ACTION 2: count() — another trigger
total_rows = selected.count()
print(f"\n>> ACTION: count() — Total completed orders: {total_rows}")

# ─────────────────────────────────────────────
# 5. AGGREGATION — Wide transformation (shuffle!)
# ─────────────────────────────────────────────
# groupBy triggers a shuffle — data moves across partitions
# This is a wide transformation

revenue_by_city = selected \
    .groupBy("city") \
    .agg(
        spark_round(spark_sum("total_value"), 2).alias("total_revenue"),
        count("order_id").alias("order_count")
    ) \
    .orderBy("total_revenue", ascending=False)

print("\n>> Revenue by City (Wide transformation — shuffle happened here):")
revenue_by_city.show(truncate=False)

revenue_by_category = selected \
    .groupBy("product_category") \
    .agg(
        spark_round(spark_sum("total_value"), 2).alias("total_revenue"),
        count("order_id").alias("order_count")
    ) \
    .orderBy("total_revenue", ascending=False)

print("\n>> Revenue by Category:")
revenue_by_category.show(truncate=False)

# ─────────────────────────────────────────────
# 6. EXPLAIN PLAN — See the DAG Spark built
# ─────────────────────────────────────────────
# This shows you the physical execution plan Spark created
# You can see how it optimized your transformations

print("\n>> Execution Plan (the DAG Spark built for revenue_by_city):")
revenue_by_city.explain(mode="simple")

# ─────────────────────────────────────────────
# 7. STOP SPARK SESSION
# ─────────────────────────────────────────────
spark.stop()
print("\n>> Spark session stopped. Day 1 complete!")