import pytest

import chispa
import eren
from .spark import spark


def test_createOrReplaceHiveView(tmp_path):
    df = spark.range(0, 5)
    path = f"{tmp_path}/some_table"
    df.write.format("delta").save(path)
    eren.createOrReplaceHiveView("some_view", path, 0)
    spark.sql("select * from some_view").show(truncate=False)
