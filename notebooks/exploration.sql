-- Databricks notebook source
-- MAGIC %md
-- MAGIC # UK Motor Vehicle Accident Data
-- MAGIC These files provide detailed road safety data about the circumstances of personal injury road accidents in GB from 2005 to 2014,
-- MAGIC 
-- MAGIC ## Content
-- MAGIC Accident file: main data set contains information about accident severity, weather, location, date, hour, day of week, road type…
-- MAGIC Vehicle file: contains information about vehicle type, vehicle model, engine size, driver sex, driver age, car age…
-- MAGIC Casualty file: contains information about casualty severity, age, sex social class, casualty type, pedestrian or car passenger…
-- MAGIC Lookup file: contains the text description of all variable code in the three files
-- MAGIC 
-- MAGIC ## License
-- MAGIC OGL:Open Government License
-- MAGIC http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/

-- COMMAND ----------

-- MAGIC %run ./resources/setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data loaded into bronze and now exploration
-- MAGIC The first thing we need to do is bring the data in to a workable format. We'll load the raw data into the initial tables where we can work with it from then on by either querying it directly, or working with it in memory via materialized views.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC First, we'll create our basic lookup tables, followed by our initial bronze data. You can load your bronze data from pretty much anywhere - this lab loads them from another database as just one example of where you can source data from.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Lets' quickly sample the data from each of our bronze (unrefined) tables. This will give us a good idea of how we might want to partition the data later on.

-- COMMAND ----------

-- DBTITLE 1,Accident Data
SELECT *
FROM b_accidents
LIMIT 5;

-- COMMAND ----------

SELECT
  COUNT(DISTINCT(`Local_Authority_-Highway-`)) as dist_highways,
  COUNT(DISTINCT(`Local_Authority_-District-`)) as dist_districts
FROM
  b_accidents

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Okay, that's awesome. Based on the number of highways and districts, we'll likely want to take the chunkier value (highways) and use that as a parition later on, and district will probably make a good z-order value.
-- MAGIC 
-- MAGIC Now let's do the same for Casualty and Vehicle data.

-- COMMAND ----------

-- DBTITLE 1,Casualty Data
SELECT *
FROM b_casualties
LIMIT 5;

-- COMMAND ----------

SELECT
  COUNT(DISTINCT(Casualty_Type)) as dist_cas_type,
  COUNT(DISTINCT(Age_Band_of_Casualty)) as dist_age_cat
FROM
  b_casualties

-- COMMAND ----------

-- DBTITLE 1,Vehicle Data
SELECT *
FROM b_vehicles
LIMIT 5;

-- COMMAND ----------

SELECT
  COUNT(DISTINCT(Vehicle_Type)) AS dist_vehicle_type,
  COUNT(DISTINCT(Age_Band_of_Driver)) as dist_age_band
FROM
  b_vehicles

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Refined and Optimized data
-- MAGIC We will next refine our data by eliminating fields and replacing null values. We'll also write out the new tables in a delta format with defined partitions and optimizations in place.

-- COMMAND ----------

SHOW COLUMNS IN b_accidents;

-- COMMAND ----------

SHOW COLUMNS IN b_casualties;

-- COMMAND ----------

SHOW COLUMNS IN b_vehicles;