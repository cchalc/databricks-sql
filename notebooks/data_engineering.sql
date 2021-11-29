-- Databricks notebook source

-- COMMAND ----------

-- MAGIC %run ./resources/setup

-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW accident_summary AS
SELECT
  Accident_Index,
  Longitude,
  Latitude,
  Accident_Severity,
  Number_of_Vehicles,
  Number_of_Casualties,
  Date,
  Day_of_Week,
  Time,
  `Local_Authority_-District-` AS la_district,
  `Local_Authority_-Highway-` AS la_highway,
  Road_Type,
  Speed_limit,
  Junction_Detail,
  Junction_Control,
  Light_Conditions,
  Weather_Conditions,
  Road_Surface_Conditions,
  Special_Conditions_at_Site,
  Urban_or_Rural_Area,
  LSOA_of_Accident_Location
FROM
  b_accidents

-- COMMAND ----------

CREATE
OR REPLACE VIEW casualty_summary AS
SELECT
  Accident_Index,
  Vehicle_Reference,
  Casualty_Reference,
  Casualty_Class,
  Age_of_Casualty,
  Age_Band_of_Casualty,
  Casualty_Severity,
  Car_Passenger,
  Casualty_Type
FROM
  b_casualties

-- COMMAND ----------

CREATE
OR REPLACE VIEW vehicle_summary AS
SELECT
  Accident_Index,
  Vehicle_Reference,
  Vehicle_Type,
  Vehicle_Manoeuvre,
  Skidding_and_Overturning,
  Hit_Object_in_Carriageway,
  Vehicle_Leaving_Carriageway,
  Hit_Object_off_Carriageway,
  Journey_Purpose_of_Driver,
  Sex_of_Driver,
  Age_of_Driver,
  Age_Band_of_Driver,
  Age_of_Vehicle
FROM
  b_vehicles

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Saving the refined data as delta tables
-- MAGIC Now that we have our refined tables in memory including the new fields, we'll write these back out refined delta tables to add a bit of permanence. It's a good idea to think of these tables as being useful for other purposes too - such as exporting to BI tools, shipping for ML etc. This is the first tier of data we will likely be reporting on in the future.

-- COMMAND ----------

-- DROP TABLE IF EXISTS s_accidents;
CREATE
OR REPLACE TABLE s_accidents USING delta PARTITIONED BY (la_highway)
SELECT
  *
FROM
  accident_summary

-- COMMAND ----------

CREATE
OR REPLACE TABLE s_casualties USING delta PARTITIONED BY (Age_Band_of_Casualty)
SELECT
  *
FROM
  casualty_summary

-- COMMAND ----------

-- DROP TABLE IF EXISTS s_vehicles;
CREATE
OR REPLACE TABLE s_vehicles USING delta PARTITIONED BY (Age_Band_of_Driver)
SELECT
  *
FROM
  vehicle_summary

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Coalesce clean data
-- MAGIC Now let's build some aggregated tables for proper BI reporting


-- COMMAND ----------

CREATE
OR REPLACE TEMPORARY VIEW mva_raw AS
SELECT
  a.Accident_Severity,
  a.Number_of_Casualties,
  a.la_highway,
  a.la_district,
  a.Weather_Conditions,
  c.Casualty_Severity,
  c.Casualty_Type,
  c.Casualty_Reference,
  c.Age_of_Casualty,
  v.Vehicle_Reference,
  v.Vehicle_Type,
  v.Vehicle_Manoeuvre,
  v.Age_of_Driver,
  cs.label as Casualty_Severity_Desc,
  ct.label as Casualty_Type_Desc,
  vt.label as Vehicle_Type_Desc,
  vm.label as Vehicle_Manoeuvre_Desc,
  w.label as Weather_Conditions_Desc
FROM
  s_accidents a
  JOIN s_casualties c ON a.Accident_Index = c.Accident_Index
  JOIN s_vehicles v ON a.Accident_Index = v.Accident_Index
  JOIN l_casualty_severity cs ON c.Casualty_Severity = cs.code
  JOIN l_casualty_type ct ON ct.code = c.Casualty_Type
  JOIN l_vehicle_type vt ON v.Vehicle_Type = vt.code
  JOIN l_vehicle_manoeuvre vm ON v.Vehicle_Manoeuvre = vm.code
  JOIN l_weather w ON w.code = a.Weather_Conditions


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Finally, now that we have a view we're happy with, let's write that out to our final table, also referred to as a 'gold' table.

-- COMMAND ----------

-- DROP TABLE IF EXISTS g_mva;
CREATE
OR REPLACE TABLE g_mva USING delta PARTITIONED BY (Casualty_Severity)
SELECT
  *
FROM
  mva_raw

