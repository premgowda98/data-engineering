# Data-engineering

<img src="https://miro.medium.com/v2/resize:fit:6068/1*JT6qEQO2zqDNOm5PN8tNHg.png" width=600>


1. OLTP vs OLAP
    1. OLAP is also called as Data Warehouse
2. ELT
3. Data Warehouse Layers
    1. Data moves from Staging to Core layer
    2. Staging is where we pull the data
        1. Transient
        2. Persistent
    3. Core is OLAP where the transformed data is stored in terms of Facts and Dimension tables
4. Incremental Loading
    1. Data fetching strategy, where only new records are pulled
5. Dimensional Modeling
    1. Star Schema
        1. Fact is at center
        2. Fact
            1. Only Numeric values
        3. Dimension
            1. Other values
        4. Data are clustered based on business use case
    2. Snowflake Schema
        1. Similar to Star, but dimension can have hierarchy of other dimensions
6. Slowly changing dimensions
    1. How frequent the data is changed
    2. 4 Types
        1. 0 -> data will not changed
        2. 1 -> Upsert
        3. 2 -> History -> For any new change new record will be created
        4. 3 -> Prev_Value -> For any new change, we store the last value
7. Data Lake
    1. Data Warehouse is for more of structured Lake
    2. Data Lake on the other data can handle structured, un-structured, semi-structured data
    3. In DW schema is defined before data is stored, whereas in DL schema is defined after data is stored
8. Data LakeHouse
    1. Combination of DW and DL
9. File Formats
    1. Row Based -> CSV, AVRO
    2. Column Based -> Parquet
    3. Delta Format
        1. Built on top of parquet
        2. ACID support
        3. Versioning
10. Big Data
    1. Frameworks
        1. Kafka
        2. Airflow -> Orchestrator
        3. Spark
        4. Databricks -> Management Layer for Spark
        5. Hive -> More like SQL
    2. Spark
        1. Distributed Computing
        2. Drive Node -> Brain of spark
        3. Worker Nodes

### Azure Data Engineer Ecosystem

1. Azure Event HUb
2. Azure Data Factory -> for ETL
3. Azure Databricks
3. Azure Synapse Analytics -> alternative to Snowflake, Redshift, BigQuery