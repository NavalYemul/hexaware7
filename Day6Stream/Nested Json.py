# Databricks notebook source
# MAGIC %run "/Workspace/Users/naval@thedatamaster.in/Hexware Training/Day5/includes"

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/hexawaredatabricks/raw/input_files/adobe_sample.json",multiLine=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df1=df.withColumn("batters",explode("batters.batter"))\
.withColumn("batters_id",col("batters.id"))\
.withColumn("batters_type",col("batters.type"))\
.drop("batters")\
.withColumn("topping",explode("topping"))\
.withColumn("topping_id",col("topping.id"))\
.withColumn("topping_type",col("topping.type"))\
.drop("topping")

# COMMAND ----------

df1.createOrReplaceTempView("adobe")

# COMMAND ----------

spark.sql("select * from adobe").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from adobe where batters_type='Chocolate'

# COMMAND ----------

Spark
Lazy Evulation
Transformation 
Action 

# COMMAND ----------

df1.where("batters_type='Chocolate'").show()

# COMMAND ----------

df1.sort(col("topping_id").desc(),"batters_type").display()

# COMMAND ----------

df1.where("batters_type='Chocolate' and topping_id=5001" ).display()

# COMMAND ----------

df1.groupBy("topping_type").count().display()

Group it by topping_id
