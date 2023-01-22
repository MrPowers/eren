# eren

This library contains Hive helper methods for PySpark users.

This README is a good way to learn about the Hive metastore for PySpark, even if you're not going to use this library directly.

![eren](https://github.com/MrPowers/eren/blob/main/images/eren.jpeg)

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


