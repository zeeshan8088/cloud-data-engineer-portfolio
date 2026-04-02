# conftest.py
# Shared SparkSession for all tests
# pytest automatically picks this up — no imports needed in test files

import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Session-scoped SparkSession fixture.
    'session' scope means ONE SparkSession is created for ALL tests
    and reused — much faster than creating one per test.
    """
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("Week9-Tests") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()