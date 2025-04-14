-- create credentials
CREATE DATABASE SCOPED CREDENTIAL myCreds
WITH IDENTITY = 'Managed Identity'


-- create external datasource
CREATE EXTERNAL DATA SOURCE source_silver
WITH(
    LOCATION = 'abfss://silver@awdatalakeuzer.dfs.core.windows.net/',
    CREDENTIAL = myCreds
)

CREATE EXTERNAL DATA SOURCE source_gold
WITH(
    LOCATION = 'abfss://gold@awdatalakeuzer.dfs.core.windows.net/',
    CREDENTIAL = myCreds
)

-- create external file format
CREATE EXTERNAL FILE FORMAT parquet_format
with(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)


-- create external table using CETAS concept
CREATE EXTERNAL TABLE gold.extsales
WITH(
    LOCATION = 'extsales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = parquet_format
) AS
SELECT * FROM gold.sales

SELECT * FROM gold.extsales