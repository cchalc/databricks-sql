# Databricks notebook source
# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

# MAGIC %run ./setup

# COMMAND ----------

# spark.sql(f"DROP DATABASE IF EXISTS {dbName} CASCADE;")

# COMMAND ----------

input_data_path = f"/FileStore/tables/{username}/{demo}"
dbutils.fs.mkdirs(input_data_path)
dbutils.fs.ls(input_data_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read in all CSV files to tables

# COMMAND ----------

import os
from os import listdir
import glob

def find_csv_filenames( path_to_dir, suffix=".csv" ):
    filenames = listdir(path_to_dir)
    return [ filename for filename in filenames if filename.endswith( suffix ) ]

# COMMAND ----------

def load_data(input_data_path, filename_delimiter):
  filenames = find_csv_filenames("/dbfs"+input_data_path)

  for file in filenames:
    table_name = file.split(filename_delimiter)[0].lower()
    
    (spark.read
     .format("csv")
     .option("inferSchema", True)
     .option("header", True)
     .option("sep", ",")
     .load(f"{input_data_path}/{file}")
     .write
     .format("delta")
     .mode("overwrite")
     .save(path + "/bronze/data/" + table_name)
    )
    
    _ = spark.sql('''
      CREATE TABLE IF NOT EXISTS `{}`.{}
      USING DELTA 
      LOCATION '{}'
      '''.format(dbName,
                 "b_" + table_name,
                 path + "/bronze/data/" + table_name)
                 )

# COMMAND ----------

load_data(input_data_path, "0514")

# COMMAND ----------

# DBTITLE 1,Load Accident Data (for reference)
# (spark.read
#  .format("csv")
#  .option("inferSchema", True)
#  .option("header", True)
#  .option("sep", ",")
#  .load("/FileStore/tables/christopherchalcraft/road_safety/Accidents0514.csv")
#  .write
#  .format("delta")
#  .mode("overwrite")
#  .save(path + "/bronze/data/accidents")
# )

# COMMAND ----------

# _ = spark.sql('''
#   CREATE TABLE IF NOT EXISTS `{}`.{}
#   USING DELTA 
#   LOCATION '{}'
#   '''.format(dbName, "accidents", path + "/bronze/data/accidents")
#              )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load look up table

# COMMAND ----------

# MAGIC %pip install xlrd

# COMMAND ----------

# MAGIC %run ./setup

# COMMAND ----------

import pandas as pd

xls_file = "/FileStore/tables/christopherchalcraft/road_safety/Road_Accident_Safety_Data_Guide.xls"
sheets = ["Weather", "Casualty Type", "Casualty Severity", "Ped Location", "Vehicle Manoeuvre", "Vehicle Type"]

# COMMAND ----------

def load_lookup(xls_file, sheets):
  df = pd.read_excel("/dbfs" + xls_file, sheet_name=None)
  
  for sheet in sheets:
    table_name = re.sub(r'\W+', '_', sheet).lower()
    
    (spark
     .createDataFrame(df[sheet])
     .write
     .format("delta")
     .mode("overwrite")
     .save(path + "/bronze/data/" + table_name)
    )
    
    _ = spark.sql('''
      CREATE TABLE IF NOT EXISTS `{}`.{}
      USING DELTA 
      LOCATION '{}'
      '''.format(dbName,
                 "l_" + table_name,
                 path + "/bronze/data/" + table_name)
                 )

# COMMAND ----------

load_lookup(xls_file, sheets)

# COMMAND ----------

# DBTITLE 1,Load Lookup Table Reference
# (spark
#  .createDataFrame(df["Weather"])
#  .write
#  .format("delta")
#  .mode("overwrite")
#  .save(path + "/bronze/data/" + "weather")
# )

# _ = spark.sql('''
#   CREATE TABLE IF NOT EXISTS `{}`.{}
#   USING DELTA 
#   LOCATION '{}'
#   '''.format(dbName, "l_weather", path + "/bronze/data/weather")
#              )
