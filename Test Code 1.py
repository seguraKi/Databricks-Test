# Databricks notebook source
# DBTITLE 1,Read Data
agr = spark.read.parquet("/mnt/mda-prod/active_agreement_fact").where("source_list_flag = 'Y'").alias('agr')

# COMMAND ----------

#TEST 1 display
display(agr.limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC
