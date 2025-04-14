# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # SILVER LAYER SCRIPT

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA ACCESS USING APP

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.awdatalakeuzer.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.awdatalakeuzer.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.awdatalakeuzer.dfs.core.windows.net", "153e3aad-0b48-4a7b-8765-b36bcd972c5c")
spark.conf.set("fs.azure.account.oauth2.client.secret.awdatalakeuzer.dfs.core.windows.net", "U6C8Q~EBwjNVwfaRZGIJO3j2Asmsnd9yV~jMccGE")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.awdatalakeuzer.dfs.core.windows.net", "https://login.microsoftonline.com/0c6e3b79-e7c8-4af8-95c5-47c1efe7ab6b/oauth2/token")


# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA LOADING

# COMMAND ----------

df_cal = (
    spark.read.option('header','true')
    .option('inferSchema','true')
    .csv("abfss://bronze@awdatalakeuzer.dfs.core.windows.net/AdventureWorks_Calendar")
)

# COMMAND ----------

df_cus = (
    spark.read.option('header','true')
    .option('inferSchema','true')
    .csv("abfss://bronze@awdatalakeuzer.dfs.core.windows.net/AdventureWorks_Customers")
)

# COMMAND ----------

df_procat = (
    spark.read.option('header','true')
    .option('inferSchema','true')
    .csv("abfss://bronze@awdatalakeuzer.dfs.core.windows.net/AdventureWorks_Product_Categories")
)

# COMMAND ----------

df_prod = (
    spark.read.option('header','true')
    .option('inferSchema','true')
    .csv("abfss://bronze@awdatalakeuzer.dfs.core.windows.net/AdventureWorks_Products")
)

# COMMAND ----------

df_ret = (
    spark.read.option('header','true')
    .option('inferSchema','true')
    .csv("abfss://bronze@awdatalakeuzer.dfs.core.windows.net/AdventureWorks_Returns")
)

# COMMAND ----------

df_sale = (
    spark.read.option('header','true')
    .option('inferSchema','true')
    .csv("abfss://bronze@awdatalakeuzer.dfs.core.windows.net/AdventureWorks_Sales*")
)

# COMMAND ----------

df_terr = (
    spark.read.option('header','true')
    .option('inferSchema','true')
    .csv("abfss://bronze@awdatalakeuzer.dfs.core.windows.net/AdventureWorks_Territories")
)

# COMMAND ----------

df_subcat = (
    spark.read.option('header','true')
    .option('inferSchema','true')
    .csv("abfss://bronze@awdatalakeuzer.dfs.core.windows.net/Product_Subcategories")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TRANSFORMATION

# COMMAND ----------

#CALENDAR TRANSFORMATION
df_cal.display()

# COMMAND ----------

df_cal = df_cal.withColumn('Month', month(col('Date')))\
    .withColumn('Year', year(col('Date')))
df_cal.display()

# COMMAND ----------

df_cal.write\
    .mode('append')\
    .parquet("abfss://silver@awdatalakeuzer.dfs.core.windows.net/AdventureWorks_Calendar")

# COMMAND ----------

#CUSTOMER DATAFRAME TRANSFORMATION
df_cus.display()

# COMMAND ----------

df_cus.withColumn('fullName', concat(col('Prefix'), lit(' '), col('FirstName'), lit(' '), col('LastName'))).display()

# COMMAND ----------

df_cus = df_cus.withColumn('fullName', concat_ws(' ', col('Prefix'), col('FirstName'), col('LastName')))

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus = df_cus\
    .withColumn('Age', date_diff(current_date(), col('BirthDate'))/365)\
    .withColumn('Age', col('Age').cast(IntegerType()))

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus.write\
    .mode('append')\
    .parquet('abfss://silver@awdatalakeuzer.dfs.core.windows.net/AdventureWorks_Customer')

# COMMAND ----------

#SUB CATEGORY DATAFRAME TRANSFORMATION
df_subcat.display()

# COMMAND ----------

#no need to transform this df
df_subcat.write\
    .mode('append')\
    .parquet('abfss://silver@awdatalakeuzer.dfs.core.windows.net/AdventureWorks_SubCategory')

# COMMAND ----------

#PRODUCT DATAFRAME TRANSFORMATION
df_prod.display()

# COMMAND ----------

df_prod = df_prod\
    .withColumn('ProductSKU', split(col('ProductSKU'), '-')[0])\
    .withColumn('ProductName', split(col('ProductName'), ' ')[0])

# COMMAND ----------

df_prod.display()

# COMMAND ----------

df_prod.write\
    .mode('append')\
    .parquet('abfss://silver@awdatalakeuzer.dfs.core.windows.net/AdventureWorks_Product')

# COMMAND ----------

#RETURNS DATAFRAME TRANSFORMATION
df_ret.display()

# COMMAND ----------

df_ret.write\
    .mode('append')\
    .parquet('abfss://silver@awdatalakeuzer.dfs.core.windows.net/AdventureWorks_Returns')

# COMMAND ----------

#TERRITORIES DATAFRAME TRANSFORMATION
df_terr.display()

# COMMAND ----------

df_terr.write\
    .mode('append')\
    .parquet('abfss://silver@awdatalakeuzer.dfs.core.windows.net/AdventureWorks_Territories')

# COMMAND ----------

#SALES DATAFRAME TRANSFORMATION
df_sale.display()

# COMMAND ----------

df_sale = df_sale.withColumn('StockDate', to_timestamp(col('StockDate')))

# COMMAND ----------



# COMMAND ----------

df_sale = df_sale.withColumn('OrderNumber', regexp_replace(col('OrderNumber'), 'S', 'T'))

# COMMAND ----------

df_sale = df_sale.withColumn('Multiply', col('OrderQuantity') * col('OrderLineItem'))

# COMMAND ----------

df_sale.display()

# COMMAND ----------

df_sale.write\
    .mode('append')\
    .parquet('abfss://silver@awdatalakeuzer.dfs.core.windows.net/AdventureWorks_Sales')

# COMMAND ----------

# MAGIC %md
# MAGIC ### SALES DATA ANALYSIS

# COMMAND ----------

df_sale.groupBy('OrderDate').agg(
    count('OrderNumber').alias('Orders')
).display()

# COMMAND ----------

df_procat.display()

# COMMAND ----------

df_terr.display()

# COMMAND ----------

