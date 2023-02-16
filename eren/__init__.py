import pyspark
from pyspark.sql import SparkSession


def hello():
    return "hi"


def createOrReplaceHiveView(view_name: str, delta_path: str, delta_version: str):
    """
    Args:
        view_name: name of new Hive view
        delta_path: path to Delta Lake Table
        delta_version: version of the Delta Lake Table

    Returns:
        Dataframe with contents of new view
    """
    spark = SparkSession.getActiveSession()
    spark.sql(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM delta.`{delta_path}@v{delta_version}`")
    return spark.sql(f"SELECT * FROM {view_name}")
