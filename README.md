# eren

This library contains Hive helper methods for PySpark users.

This README is a good way to learn about the Hive metastore for PySpark, even if you're not going to use this library directly.

![eren](https://github.com/MrPowers/eren/raw/main/images/eren.jpeg)

## Hive metastore background

The Hive metastore contains information on tables, where the data is located, and the schema of the data.

Registering tables in the Hive metastore is a great way to make nice Spark workflows for an organization.  You can setup your Spark cluster to automatically regsiter all your organization's tables, so users can spin up clusters and immediately start querying the data with SQL syntax.

Suppose you have a table called `important_customers` and a database analyst that would like to query this table to generate a report for the accounting department on a weekly basis.  If the `important_customers` table is registered in the Hive metastore, then the analyst can spin up a Spark cluster and run their queries, without having to worry about where the underlying data is stored or the schema.

## How this library helps

This library contains helper methods to make it easier to manage your Hive metastore.  You can use this libray to easily determine if a Hive table is managed, unmanaged, or non-registered for example.

You can also use this library to register Hive tables that are not currently in the metastore.

This library makes common Hive tasks for PySpark programmers easy.

## Hive managed vs unmanaged vs non-registered tables

Hive managed tables are registered in the Hive metastore and have paths that are managed by Hive.  Here's an example of how to create a managed table.

```python

```

This table stores data in XX.



## Hive Parquet vs Delta Lake tables

## Working with Hadoop File System from Python

`Eren` also provides a top level interface for working with any implementation of HadoopFileSystem (ibcluding S3, HDFS/DBFS and Local File System).

```python
from eren import fs

# spark: SparkSession

hdfs = fs.HadoopFileSystem(spark, "hdfs:///data") # returns an instance for access to HDFS
s3fs = fs.HadoopFileSystem(spark, "s3a:///bucket/prefix") # returns an instance for access to S3
local_fs = fs.HadoopFileSystem(spark, "file:///home/my-home") # return an instance for access to LocalFS
```

### Glob Files

You can list files and directories on remote file system:

```python
list_of_files = hdfs.glob("hdfs:///raw/table/**/*.csv") # list all CSV files recursively inside a folder on HDFS
```

### Read/Write UTF-8 strings

You can read and write UTF-8 strings from/to remote file system. But be carefully! This method is extremely slow compared to regular `spark.read`. Do not use this for reading/writing data!

```python
import json

hdfs.write_utf8("hdfs:///data/results/results.json", json.dumps({"key1": 1, "key2": 3}), mode="w")
json.loads(hdfs.read_utf8("hdfs:///data/results/results.json"))
```

## Community

### Blogs

- [Semyon Sinchenko: Working With File System from PySpark](https://semyonsinchenko.github.io/ssinchenko/post/working-with-fs-pyspark/)


