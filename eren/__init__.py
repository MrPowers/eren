import pyspark
from pyspark.sql import SparkSession


def hello():
    return "hi"


def createOrReplaceHiveView(viewName, deltaPath, deltaVersion):
    query = f"CREATE OR REPLACE VIEW {viewName} AS SELECT * FROM delta.`{deltaPath}@v{deltaVersion}`"
    return SparkSession.getActiveSession().sql(query)