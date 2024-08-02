# Databricks notebook source
# MAGIC %md
# MAGIC ## Structured Streaming

# COMMAND ----------

Batch
Read
df=spark.read.csv("path")
Write
df.write.mode("append").saveAsTable("tblname")


Real-time (Data which grow)
Spark Structured Streaming
1. Exactly graunteed that it will read only once 
2. default append


df=spark.readStream.csv("path")
df.writeStream.option("checkpointLocation","path").table("tblname")

Limitation:
    1.Define your schema
    2.Schema should be fixed

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/hexawaredatabricks/raw/stream_in/

# COMMAND ----------

df=spark.readStream.csv("dbfs:/mnt/hexawaredatabricks/raw/stream_in/Jan.CSV")

# COMMAND ----------

from pyspark.sql.types import *
users_schema=StructType([StructField("Id", IntegerType()),
                         StructField("Name", StringType()),
                         StructField("Gender", StringType()),
                         StructField("Salary", IntegerType()),
                         StructField("Country", StringType()),
                         StructField("Date", StringType())
])
df=spark.readStream.schema(users_schema).csv("dbfs:/mnt/hexawaredatabricks/raw/stream_in/",header=True)

# COMMAND ----------

df=spark.readStream.schema(users_schema).csv("dbfs:/mnt/hexawaredatabricks/raw/stream_in/",header=True)

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

# MAGIC %sql
# MAGIC describe history datamaster.bronze.stream

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from datamaster.bronze.stream

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Notified
# MAGIC -- new column
# MAGIC -- failed uploaded new data -
