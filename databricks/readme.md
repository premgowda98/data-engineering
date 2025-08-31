## Databricks

1. [Spark Arch Explained](https://youtu.be/jDkLiqlyQaY)

### Snowflake vs Databricks

1. Snowflake
    1. Built for structured data
    2. Data WareHouse Model
    3. Best suited for BI reporting and Analytics
2. Databricks
    1. Designed for Big data and machine learning
    2. Data Lake house model


### Databricks

1. Magic Commands
    1. %sql, %python %r, %md
2. DBFS (Databricks File System)
    1. In order to access the data from data lake
3. Databricks Utilities
    1. dbutils.fs -> lists all folders in data lake
    2. dbutils.widgets
    3. dbutils.secrets
4. Delta Lake
    1. Format in parquet along with transaction log / delta log
        1. This delta log has all the metadata of the tables and data
    2. Managed Delta Tables
        1. MetaStore -> for Metadata info of tables
        2. Actual data is stored in databricks managed storage
    3. External Delta Tables
        1. Here the data is stored in the cloud storage we configured
    4. The Meta store can be seen in the catalog section in databricks UI
    5. Delta tables have similar query language like SQL
    6. Deletes are tombstone.
    7. To describe version history `DESCRIBE HISTORY db.table`
    8. To restore `RESTORE TABLE db.table TO VERSION AS OF 2`
    9. Vaccum command deletes the unused partition, only when the partition age is > 7 days `VACCUM db.table`.
    10. To drop partitions immediately use `VACCUM db.table RETAIN 0 HOURS`
5. Workflows
    1. To create pipeline for running notebooks
6. Compute
    1. To configure the cluster for spark
    2. [Unity Catalog](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/)
        1. Data governance solution 
        2. One metastore for one region
        <image src="https://learn.microsoft.com/en-us/azure/databricks/_static/images/unity-catalog/with-unity-catalog.png">
        3. With Unity Catalog Hive metastore is not used instead a central Unity Metastore is used
        4. Without Unity Catalog, each Databricks workspace had it's metastore called Hive Metastore