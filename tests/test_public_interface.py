import pytest
import chispa
import pyspark
import eren
from pyspark.sql import SparkSession
from delta import DeltaTable, configure_spark_with_delta_pip


builder = (
    pyspark.sql.SparkSession.builder.appName("MyApp")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.shuffle.partitions", "2")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

def test_hello():
    eren.hello()


def test_createOrReplaceHiveView(tmp_path):
    df = spark.range(0, 5)
    path = f"{tmp_path}/some_table"
    df.write.format("delta").save(path)
    eren.createOrReplaceHiveView("some_view", path, 0)
    spark.sql("select * from some_view").show(truncate=False)
