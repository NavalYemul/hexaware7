# Databricks notebook source
# MAGIC %run "/Workspace/Users/naval@thedatamaster.in/Hexware Training/Day5/includes"

# COMMAND ----------

dbutils.widgets.text("environment"," ")
w=dbutils.widgets.get("environment")

# COMMAND ----------

# DBTITLE 1,Reading
df=spark.read.csv(f"{input}",header=True,inferSchema=True)

# COMMAND ----------

# DBTITLE 1,Transformation
df1=add_ingestion(df)

# COMMAND ----------

df1.columns

# COMMAND ----------

new_col=['name', 'country', 'industry', 'net_worth_in_billions', 'company','ingestion_date']

# COMMAND ----------

df2=df1.toDF(*new_col)

# COMMAND ----------

df3=df2.withColumn("environment",lit(w))

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog datamaster;
# MAGIC create schema if not exists bronze;
# MAGIC use bronze;

# COMMAND ----------

df3.write.mode("overwrite").option("mergeSchema","true").saveAsTable("richest_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from richest_bronze
