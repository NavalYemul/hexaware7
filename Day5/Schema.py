# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines/

# COMMAND ----------

df_airlines=spark.read.csv("dbfs:/databricks-datasets/asa/airlines/",header=True)

# COMMAND ----------

df_airline=spark.read.csv("dbfs:/databricks-datasets/asa/airlines/",header=True,inferSchema=True)

# COMMAND ----------

df_airlines.count()

# COMMAND ----------

User defined Schema(Table schema) 
str,
list,
pyspark

# COMMAND ----------

# MAGIC %run "/Workspace/Users/naval@thedatamaster.in/Hexware Training/Day5/includes"

# COMMAND ----------

df=spark.read.csv(f"{input}",header=True,inferSchema=True)

# COMMAND ----------

str_schema="name string, country String, industry string, net_worth_in_billion double, company string"

# COMMAND ----------

df_new=spark.read.schema(str_schema).csv(f"{input}",header=True)

# COMMAND ----------

df_new.display()

# COMMAND ----------

data={"id":1,"name":"naval"}
schema="id int, name string"

# COMMAND ----------

data={"id":1,"name":"naval","mobile":[123,976]}
schema="id int, name string, mobile array"(NOT WORK)

# COMMAND ----------

data={"id":1,"name":"naval","mobile":{"home":123,"office":976}}
schema="id int, name string, mobile map"(NOT WORK)

# COMMAND ----------

pyspark DataType
1. Struct
2. Array
3. Map

# COMMAND ----------

str_schema="name string, country String, industry string, net_worth_in_billion double, company string"

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

from pyspark.sql.types import *
pyspark_schmema= StructType([StructField("name",StringType()),
                             StructField("country",StringType()),
                             StructField("industry",StringType()),
                             StructField("net_worth",DoubleType()),
                             StructField("company",StringType())
])


# COMMAND ----------

df_new2=spark.read.schema(pyspark_schmema).csv(f"{input}",header=True)
