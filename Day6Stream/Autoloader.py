# Databricks notebook source
https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/

# COMMAND ----------

(
spark
 .readStream
 .schema(users_schema)
 .csv("dbfs:/mnt/hexawaredatabricks/raw/stream_in/",header=True)
 .writeStream
 .option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/YourName/stream")
 .trigger(once=True)
 .table("datamaster.bronze.stream")
 )

# COMMAND ----------

(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/naval/autoloader")
.trigger(once=True)
.table("datamaster.bronze.autoloader")
)

# COMMAND ----------

(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/naval/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/naval/autoloader")
.table("datamaster.bronze.autoloader")
)

# COMMAND ----------

(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/naval/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/naval/autoloader")
.table("datamaster.bronze.autoloader")
)

# COMMAND ----------

(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/naval/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/naval/autoloader")
.option("mergeSchema",True)
.table("datamaster.bronze.autoloader")
)

# COMMAND ----------

# DBTITLE 1,Final Autoloader
(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaEvolutionMode","_____")
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/____/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/_____/autoloader")
.option("mergeSchema",True)
.table("_________")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from datamaster.bronze.autoloader

# COMMAND ----------

df=spark.read.table("datamaster.bronze.autoloader")

# COMMAND ----------

# MAGIC %sql
# MAGIC select Country, count(*) from datamaster.bronze.autoloader group by Country

# COMMAND ----------

Schema Evolution:
1. New column---I should get notified (decide-- get a new column or drop the source)
2.
