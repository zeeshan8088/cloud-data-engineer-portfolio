# test_etl_pipeline.py
# Unit tests for each ETL transformation function
# Week 9 - Day 3 | cloud-data-engineer-portfolio

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'jobs'))

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from datetime import date

from day3_etl_pipeline import clean_orders, transform_orders, build_tier_report, ORDERS_SCHEMA


def make_orders_df(spark, rows):
    """Helper: create a test DataFrame from a list of dicts."""
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", StringType(), True),
        StructField("city", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("order_date", StringType(), True),
        StructField("status", StringType(), True),
    ])
    return spark.createDataFrame(rows, schema)


# ─────────────────────────────────────────────
# CLEAN TESTS
# ─────────────────────────────────────────────

def test_clean_removes_null_order_ids(spark):
    """Rows with null order_id should be dropped."""
    rows = [
        (1001, "C001", "bengaluru", "Electronics", 2, 5000.0, "2024-01-15", "completed"),
        (None, "C002", "Mumbai", "Clothing", 1, 1000.0, "2024-01-16", "completed"),
    ]
    df = make_orders_df(spark, rows)
    result = clean_orders(df)
    assert result.count() == 1


def test_clean_uppercases_city(spark):
    """City names should be converted to uppercase."""
    rows = [
        (1001, "C001", "bengaluru", "Electronics", 2, 5000.0, "2024-01-15", "completed"),
        (1002, "C002", "mumbai", "Clothing", 1, 1000.0, "2024-01-16", "completed"),
    ]
    df = make_orders_df(spark, rows)
    result = clean_orders(df)
    cities = [row.city for row in result.select("city").collect()]
    assert all(c == c.upper() for c in cities), f"Cities not uppercased: {cities}"


def test_clean_removes_duplicates(spark):
    """Duplicate order_ids should be removed, keeping one."""
    rows = [
        (1001, "C001", "Bengaluru", "Electronics", 2, 5000.0, "2024-01-15", "completed"),
        (1001, "C001", "Bengaluru", "Electronics", 2, 5000.0, "2024-01-15", "completed"),
        (1002, "C002", "Mumbai", "Clothing", 1, 1000.0, "2024-01-16", "completed"),
    ]
    df = make_orders_df(spark, rows)
    result = clean_orders(df)
    assert result.count() == 2


def test_clean_fills_null_status(spark):
    """Null status should be replaced with 'unknown'."""
    rows = [
        (1001, "C001", "Bengaluru", "Electronics", 2, 5000.0, "2024-01-15", None),
    ]
    df = make_orders_df(spark, rows)
    result = clean_orders(df)
    status = result.select("status").first()["status"]
    assert status == "unknown", f"Expected 'unknown', got '{status}'"


# ─────────────────────────────────────────────
# TRANSFORM TESTS
# ─────────────────────────────────────────────

def test_transform_calculates_total_value(spark):
    """total_value should be quantity * unit_price rounded to 2dp."""
    rows = [
        (1001, "C001", "BENGALURU", "Electronics", 3, 1000.0, "2024-01-15", "completed"),
    ]
    df = make_orders_df(spark, rows)
    cleaned = clean_orders(df)
    result = transform_orders(cleaned)
    total = result.select("total_value").first()["total_value"]
    assert total == 3000.0, f"Expected 3000.0, got {total}"


def test_transform_high_value_flag(spark):
    """Orders > 10000 total should be flagged YES, others NO."""
    rows = [
        (1001, "C001", "BENGALURU", "Electronics", 1, 50000.0, "2024-01-15", "completed"),
        (1002, "C002", "MUMBAI", "Clothing", 1, 500.0, "2024-01-15", "completed"),
    ]
    df = make_orders_df(spark, rows)
    cleaned = clean_orders(df)
    result = transform_orders(cleaned)
    flags = {r.order_id: r.is_high_value for r in result.select("order_id", "is_high_value").collect()}
    assert flags[1001] == "YES"
    assert flags[1002] == "NO"


def test_transform_value_tier(spark):
    """Value tier should correctly assign Gold/Silver/Bronze."""
    rows = [
        (1001, "C001", "BENGALURU", "Electronics", 1, 60000.0, "2024-01-15", "completed"),
        (1002, "C002", "MUMBAI", "Electronics", 1, 15000.0, "2024-01-15", "completed"),
        (1003, "C003", "DELHI", "Clothing", 1, 500.0, "2024-01-15", "completed"),
    ]
    df = make_orders_df(spark, rows)
    cleaned = clean_orders(df)
    result = transform_orders(cleaned)
    tiers = {r.order_id: r.value_tier for r in result.select("order_id", "value_tier").collect()}
    assert tiers[1001] == "Gold"
    assert tiers[1002] == "Silver"
    assert tiers[1003] == "Bronze"


def test_transform_adds_month_year(spark):
    """order_month and order_year should be extracted correctly."""
    rows = [
        (1001, "C001", "BENGALURU", "Electronics", 1, 1000.0, "2024-06-15", "completed"),
    ]
    df = make_orders_df(spark, rows)
    cleaned = clean_orders(df)
    result = transform_orders(cleaned)
    row = result.select("order_month", "order_year").first()
    assert row["order_month"] == 6
    assert row["order_year"] == 2024


# ─────────────────────────────────────────────
# AGGREGATE TESTS
# ─────────────────────────────────────────────

def test_tier_report_excludes_non_completed(spark):
    """Tier report should only include completed orders."""
    rows = [
        (1001, "C001", "BENGALURU", "Electronics", 1, 60000.0, "2024-01-15", "completed"),
        (1002, "C002", "MUMBAI", "Electronics", 1, 60000.0, "2024-01-15", "cancelled"),
        (1003, "C003", "DELHI", "Electronics", 1, 60000.0, "2024-01-15", "returned"),
    ]
    df = make_orders_df(spark, rows)
    cleaned = clean_orders(df)
    transformed = transform_orders(cleaned)
    report = build_tier_report(transformed)
    total_orders = report.agg({"order_count": "sum"}).first()[0]
    assert total_orders == 1, f"Expected 1 completed order, got {total_orders}"