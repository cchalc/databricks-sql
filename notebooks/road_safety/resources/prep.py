# Databricks notebook source
# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

# MAGIC %run ./setup

# COMMAND ----------

input_data_path = f"/FileStore/tables/{username}/{demo}"
dbutils.fs.mkdirs(input_data_path)
dbutils.fs.ls(input_data_path)

# COMMAND ----------


