# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists datamaster.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace table silver.richest_silver as 
# MAGIC select name, country, industry, net_worth_in_billions, company from datamaster.bronze.richest_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists datamaster.gold;
# MAGIC use gold

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table gold.country_count as  
# MAGIC select country, count(country) as count from datamaster.silver.richest_silver group by country order by count desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.country_count

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table gold.name_count as  
# MAGIC select name, count(country) as count from datamaster.silver.richest_silver group by name order by count desc
