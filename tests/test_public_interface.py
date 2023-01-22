import pytest
import chispa
import pyspark
import eren
from pyspark.sql import SparkSession


builder = (
    pyspark.sql.SparkSession.builder.appName("MyApp")
    .config("spark.sql.shuffle.partitions", "2")
)

spark = SparkSession.builder \
  .master("local") \
  .appName("eren") \
  .getOrCreate()


def test_hello():
    eren.hello()


def test_hello():
    eren.createOrReplaceHiveView("some_view", "tmp", 0)