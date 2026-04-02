# day5_performance.py
# Spark performance optimization — broadcast joins, caching, AQE, partition pruning
# Week 9 - Day 5 | cloud-data-engineer-portfolio

import os
import sys
import time

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, upper, to_date, when, round as spark_round,
    month, year, datediff, current_date,
    sum as spark_sum, count, avg, broadcast,
    randn, abs as spark_abs, lit, rand
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType
)
from pyspark.storagelevel import StorageLevel


# ─────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Week9-Day5-Performance") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

# NOTE: autoBroadcastJoinThreshold = -1 DISABLES automatic broadcast
# so we can demonstrate the difference manually. In production you'd
# leave it at the default (10MB) or increase it.

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("Day 5 — Spark Performance Optimization")
print("=" * 60)


# ─────────────────────────────────────────────
# LOAD DATA
# ─────────────────────────────────────────────
ORDERS_SCHEMA = StructType([
    StructField("order_id",         IntegerType(), True),
    StructField("customer_id",      StringType(),  True),
    StructField("city",             StringType(),  True),
    StructField("product_category", StringType(),  True),
    StructField("quantity",         IntegerType(), True),
    StructField("unit_price",       DoubleType(),  True),
    StructField("order_date",       StringType(),  True),
    StructField("status",           StringType(),  True),
])

orders_df = spark.read \
    .option("header", "true") \
    .schema(ORDERS_SCHEMA) \
    .csv("data/orders_large.csv") \
    .withColumn("city", upper(col("city"))) \
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
    .withColumn("total_value", spark_round(col("quantity") * col("unit_price"), 2)) \
    .filter(col("order_id").isNotNull()) \
    .dropDuplicates(["order_id"])

# Small lookup table — city metadata
city_data = [
    ("BENGALURU",  "Karnataka",     "Tier1"),
    ("MUMBAI",     "Maharashtra",   "Tier1"),
    ("DELHI",      "Delhi",         "Tier1"),
    ("CHENNAI",    "Tamil Nadu",    "Tier1"),
    ("HYDERABAD",  "Telangana",     "Tier1"),
    ("PUNE",       "Maharashtra",   "Tier2"),
    ("KOLKATA",    "West Bengal",   "Tier2"),
    ("AHMEDABAD",  "Gujarat",       "Tier2"),
    ("JAIPUR",     "Rajasthan",     "Tier2"),
    ("SURAT",      "Gujarat",       "Tier2"),
]
city_df = spark.createDataFrame(city_data, ["city", "state", "city_tier"])

print(f"\nOrders loaded: {orders_df.count()} rows")
print(f"City lookup: {city_df.count()} rows")


# ─────────────────────────────────────────────
# OPTIMIZATION 1: SHUFFLE JOIN vs BROADCAST JOIN
# ─────────────────────────────────────────────
print("\n" + "=" * 60)
print("OPTIMIZATION 1: Shuffle Join vs Broadcast Join")
print("=" * 60)

# --- Shuffle Join (autoBroadcast disabled above) ---
print("\n>> Shuffle Join (no broadcast):")
t0 = time.time()

shuffle_join = orders_df.join(city_df, on="city", how="left")
shuffle_count = shuffle_join.count()

shuffle_time = time.time() - t0
print(f"   Result rows: {shuffle_count}")
print(f"   Time: {shuffle_time:.2f}s")

print("\n   Execution plan (look for SortMergeJoin — that's the shuffle):")
shuffle_join.explain(mode="simple")

# --- Broadcast Join ---
print("\n>> Broadcast Join (city_df sent to every executor):")
t0 = time.time()

broadcast_join = orders_df.join(broadcast(city_df), on="city", how="left")
broadcast_count = broadcast_join.count()

broadcast_time = time.time() - t0
print(f"   Result rows: {broadcast_count}")
print(f"   Time: {broadcast_time:.2f}s")

print("\n   Execution plan (look for BroadcastHashJoin — NO shuffle):")
broadcast_join.explain(mode="simple")

print(f"\n>> RESULT: Broadcast join was {shuffle_time/broadcast_time:.1f}x faster")
print("   In production: always broadcast small lookup tables (<10MB)")


# ─────────────────────────────────────────────
# OPTIMIZATION 2: CACHING
# ─────────────────────────────────────────────
print("\n" + "=" * 60)
print("OPTIMIZATION 2: Caching vs No Cache")
print("=" * 60)

# Expensive base DataFrame — join + filter + transform
enriched_df = broadcast_join \
    .filter(col("status") == "completed") \
    .withColumn("total_value", spark_round(col("quantity") * col("unit_price"), 2)) \
    .withColumn("value_tier",
        when(col("total_value") >= 50000, "Gold")
        .when(col("total_value") >= 10000, "Silver")
        .otherwise("Bronze")
    )

# --- Without cache: DataFrame recomputed 3 times ---
print("\n>> Without cache (DataFrame recomputed on each action):")
t0 = time.time()

r1 = enriched_df.groupBy("city").agg(spark_round(spark_sum("total_value"), 2).alias("revenue")).count()
r2 = enriched_df.groupBy("value_tier").agg(count("order_id").alias("cnt")).count()
r3 = enriched_df.groupBy("product_category").agg(spark_round(avg("total_value"), 2).alias("avg_val")).count()

no_cache_time = time.time() - t0
print(f"   3 aggregations completed in: {no_cache_time:.2f}s")
print("   (enriched_df was recomputed from scratch 3 times)")

# --- With cache: computed once, reused ---
print("\n>> With cache (DataFrame computed once, reused 3 times):")
enriched_df.cache()

# Trigger caching with first action
_ = enriched_df.count()

t0 = time.time()

r1 = enriched_df.groupBy("city").agg(spark_round(spark_sum("total_value"), 2).alias("revenue")).count()
r2 = enriched_df.groupBy("value_tier").agg(count("order_id").alias("cnt")).count()
r3 = enriched_df.groupBy("product_category").agg(spark_round(avg("total_value"), 2).alias("avg_val")).count()

cache_time = time.time() - t0
print(f"   3 aggregations completed in: {cache_time:.2f}s")
print("   (enriched_df was read from memory cache)")

print(f"\n>> RESULT: Cache was {no_cache_time/cache_time:.1f}x faster for repeated use")
print("   Rule: cache when a DataFrame is used 2+ times AND expensive to compute")

# Always unpersist when done — free up memory
enriched_df.unpersist()
print("   enriched_df unpersisted — memory freed")


# ─────────────────────────────────────────────
# OPTIMIZATION 3: AQE IN ACTION
# ─────────────────────────────────────────────
print("\n" + "=" * 60)
print("OPTIMIZATION 3: AQE — Adaptive Query Execution")
print("=" * 60)

# Re-enable auto broadcast for AQE demo
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB

aqe_df = orders_df.join(city_df, on="city", how="left") \
    .groupBy("city", "state", "city_tier") \
    .agg(
        spark_round(spark_sum("total_value"), 2).alias("total_revenue"),
        count("order_id").alias("order_count")
    )

print("\n>> AQE execution plan (isFinalPlan=false means AQE will adapt at runtime):")
aqe_df.explain(mode="simple")

print("\n>> Triggering action — AQE adapts plan based on actual data...")
aqe_df.show(10, truncate=False)

print("""
AQE did these things automatically:
1. Saw city_df is tiny → switched to BroadcastHashJoin (no shuffle for join)
2. Saw shuffle output is small → coalesced 8 shuffle partitions into fewer
3. Checked for skew → no skew detected in this dataset
""")


# ─────────────────────────────────────────────
# OPTIMIZATION 4: PARTITION PRUNING DEMO
# ─────────────────────────────────────────────
print("\n" + "=" * 60)
print("OPTIMIZATION 4: Writing Partitioned Data + Partition Pruning")
print("=" * 60)

import pathlib
pathlib.Path("output/day5_partitioned").mkdir(parents=True, exist_ok=True)

# Write partitioned by city (creates subfolders per city)
print("\n>> Writing orders partitioned by city...")
enriched_for_write = orders_df \
    .filter(col("status") == "completed") \
    .withColumn("total_value", spark_round(col("quantity") * col("unit_price"), 2))

enriched_for_write \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .partitionBy("city") \
    .csv("output/day5_partitioned")

print(">> Partitioned CSV written. Folder structure:")
import os
for entry in sorted(os.listdir("output/day5_partitioned")):
    print(f"   output/day5_partitioned/{entry}/")

# Read back with partition filter — Spark only reads BENGALURU folder
print("\n>> Reading back with filter on city (partition pruning):")
pruned_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("output/day5_partitioned") \
    .filter(col("city") == "BENGALURU")

pruned_count = pruned_df.count()
print(f"   Bengaluru orders: {pruned_count}")
print("   Spark only read the BENGALURU partition folder — skipped all others")
print("   On TB-scale data this reduces read volume by 90%+")


# ─────────────────────────────────────────────
# OPTIMIZATION 5: SKEW DETECTION
# ─────────────────────────────────────────────
print("\n" + "=" * 60)
print("OPTIMIZATION 5: Data Skew Detection")
print("=" * 60)

print("\n>> Order distribution by city (checking for skew):")
skew_check = orders_df \
    .groupBy("city") \
    .agg(count("order_id").alias("order_count")) \
    .orderBy("order_count", ascending=False)

skew_check.show(truncate=False)

total_orders = orders_df.count()
top_city = skew_check.first()
pct = (top_city["order_count"] / total_orders) * 100

print(f"\n   Top city '{top_city['city']}' has {top_city['order_count']} orders ({pct:.1f}% of total)")
if pct > 30:
    print("   ⚠ SKEW DETECTED — one city dominates. In a real cluster,")
    print("   this partition would be much slower than others.")
    print("   Fix: AQE skew join, or add a salt column to spread the data.")
else:
    print("   ✓ Distribution is reasonably balanced — no severe skew.")


# ─────────────────────────────────────────────
# INTERVIEW Q&A SUMMARY
# ─────────────────────────────────────────────
print("\n" + "=" * 60)
print("INTERVIEW Q&A — Key Answers From Today")
print("=" * 60)
print("""
Q: When would you use a broadcast join?
A: When one side of a join is small enough to fit in executor memory
   (typically <10MB). Spark sends a copy to every executor, eliminating
   the shuffle entirely. Force it with broadcast(df) or set
   spark.sql.autoBroadcastJoinThreshold.

Q: What's the difference between cache() and persist()?
A: cache() is shorthand for persist(MEMORY_ONLY). persist() lets you
   choose the storage level — MEMORY_AND_DISK spills to disk if memory
   is full, making it safer for large DataFrames.

Q: What does AQE do?
A: AQE (Adaptive Query Execution, Spark 3.x) adjusts the query plan
   at runtime based on actual data statistics. It auto-coalesces shuffle
   partitions, switches to broadcast joins when it detects a small table,
   and splits skewed partitions automatically.

Q: What is partition pruning?
A: When data is stored with partitionBy(column), Spark only reads the
   relevant partition folders when filtering on that column — skipping
   irrelevant data entirely at the storage layer.

Q: What is data skew and how do you fix it?
A: Skew is when one partition has significantly more data than others,
   causing that executor to become a bottleneck. Fixes: enable AQE skew
   join optimization, use salting (add a random prefix to the join key
   to spread data), or repartition before the operation.
""")

spark.stop()
print(">> Day 5 complete!")