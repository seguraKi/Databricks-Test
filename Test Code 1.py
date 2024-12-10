# Databricks notebook source
# DBTITLE 1,Read Data
agr = spark.read.parquet("/mnt/mda-prod/active_agreement_fact").where("source_list_flag = 'Y'").alias('agr')

# COMMAND ----------

#TEST 1 display
display(agr.limit(1))

# COMMAND ----------

#TEST 2 count
total_count = agr.count()
print("Total Rows: " + total_count)

# COMMAND ----------

# MAGIC %md
# MAGIC
