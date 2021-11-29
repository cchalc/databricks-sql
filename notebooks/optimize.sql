-- Databricks notebook source

-- COMMAND ----------

-- MAGIC %run ./resources/setup

-- COMMAND ----------

OPTIMIZE s_accidents ZORDER BY (la_district)

-- COMMAND ----------

OPTIMIZE s_casualties ZORDER BY (Casualty_Type)

-- COMMAND ----------

OPTIMIZE s_vehicles ZORDER BY (Vehicle_Type)

-- COMMAND ----------

OPTIMIZE g_mva ZORDER BY (Vehicle_Type)
