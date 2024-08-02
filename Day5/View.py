# Databricks notebook source
Views: A virtual Table

Std Views: Parsisted (SQL)

Temp Views: valid till the spark session and cluster(SQL and PySpark)

Global Temp View: valid till cluster (SQL, PySpark)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from richest_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.richest_globalview

# COMMAND ----------


