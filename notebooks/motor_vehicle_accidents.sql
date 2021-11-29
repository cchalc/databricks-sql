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

-- MAGIC %md
-- MAGIC ## Setup Area
-- MAGIC 
-- MAGIC NOTE: We're only doing a wee bit of python here to make setup easier. The rest of this notebook will be all SQL all the time :)
-- MAGIC 
-- MAGIC 1. Make sure to use your student ID as part of your login. eg. odl_student_401035@databricks.com - the student ID is 401035
-- MAGIC 1. Fill in your student id in the widget at the top of this notebook before running anything
-- MAGIC 1. Run all three cells in order for the databse setup.
-- MAGIC 1. If your cluster instance stops, make sure you run cell 9 over again to make sure you're using the right database instance.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC widget setup

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #dbutils.widgets.removeAll()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #dbutils.widgets.text("user_id", defaultValue="", label="user_id")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC database setup

-- COMMAND ----------

-- MAGIC %python
-- MAGIC user_id = dbutils.widgets.get("user_id")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"DROP DATABASE IF EXISTS {user_id}_dbacademy CASCADE;")
-- MAGIC spark.sql(f"CREATE DATABASE {user_id}_dbacademy")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"USE {user_id}_dbacademy;")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Loading the initial data into bronze tables
-- MAGIC The first thing we need to do is bring the data in to a workable format. We'll load the raw data into the initial tables where we can work with it from then on by either querying it directly, or working with it in memory via materialized views.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC First, we'll create our basic lookup tables, followed by our initial bronze data. You can load your bronze data from pretty much anywhere - this lab loads them from another database as just one example of where you can source data from.

-- COMMAND ----------

-- DBTITLE 1,Lookups
DROP TABLE IF EXISTS l_casualty_severity;
DROP TABLE IF EXISTS l_casualty_type;
DROP TABLE IF EXISTS l_ped_location;
DROP TABLE IF EXISTS l_vehicle_manoeuvre;
DROP TABLE IF EXISTS l_vehicle_type;
DROP TABLE IF EXISTS l_weather;

CREATE TABLE l_casualty_severity
SELECT * FROM default.casualty_severity;

CREATE TABLE l_casualty_type
SELECT * FROM default.casualty_type;

CREATE TABLE l_ped_location
SELECT * FROM default.ped_location;

CREATE TABLE l_vehicle_manoeuvre
SELECT * FROM default.vehicle_manoeuvre;

CREATE TABLE l_vehicle_type
SELECT * FROM default.vehicle_type;

CREATE TABLE l_weather
SELECT * FROM default.weather;

-- COMMAND ----------

-- DBTITLE 1,Content
DROP TABLE IF EXISTS b_accidents;
DROP TABLE IF EXISTS b_casualties;
DROP TABLE IF EXISTS b_vehicles;

CREATE TABLE b_accidents
SELECT * FROM default.accidents;

CREATE TABLE b_casualties
SELECT * FROM default.casualties;

CREATE TABLE b_vehicles
SELECT * FROM default.vehicles;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Lets' quickly sample the data from each of our bronze (unrefined) tables. This will give us a good idea of how we might want to partition the data later on.

-- COMMAND ----------

-- DBTITLE 1,Accident Data
SELECT *
FROM b_accidents
LIMIT 5;

-- COMMAND ----------

SELECT COUNT(DISTINCT(`Local_Authority_-Highway-`)) as dist_highways, COUNT(DISTINCT(`Local_Authority_-District-`)) as dist_districts
FROM b_accidents

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

SELECT COUNT(DISTINCT(Casualty_Type)) as dist_cas_type, COUNT(DISTINCT(Age_Band_of_Casualty)) as dist_age_cat
FROM b_casualties

-- COMMAND ----------

-- DBTITLE 1,Vehicle Data
SELECT *
FROM b_vehicles
LIMIT 5;

-- COMMAND ----------

SELECT COUNT(DISTINCT(Vehicle_Type)) AS dist_vehicle_type, COUNT(DISTINCT(Age_Band_of_Driver)) as dist_age_band
FROM b_vehicles

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Refined and Optimized data
-- MAGIC We will next refine our data by eliminating fields and replacing null values. We'll also write out the new tables in a delta format with defined partitions and optimizations in place.

-- COMMAND ----------

SHOW COLUMNS IN b_accidents;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW accident_summary
AS SELECT Accident_Index, Longitude, Latitude,
  Accident_Severity, Number_of_Vehicles, Number_of_Casualties,
  Date, Day_of_Week, Time, `Local_Authority_-District-` AS la_district,
  `Local_Authority_-Highway-` AS la_highway, Road_Type, Speed_limit,
  Junction_Detail, Junction_Control, Light_Conditions, Weather_Conditions,
  Road_Surface_Conditions, Special_Conditions_at_Site, Urban_or_Rural_Area,
  LSOA_of_Accident_Location
FROM b_accidents

-- COMMAND ----------

SHOW COLUMNS IN b_casualties;

-- COMMAND ----------

CREATE OR REPLACE VIEW casualty_summary
AS SELECT Accident_Index, Vehicle_Reference, Casualty_Reference,
  Casualty_Class, Age_of_Casualty, Age_Band_of_Casualty,
  Casualty_Severity, Car_Passenger, Casualty_Type
FROM b_casualties

-- COMMAND ----------

SHOW COLUMNS IN b_vehicles;

-- COMMAND ----------

CREATE OR REPLACE VIEW vehicle_summary
AS SELECT Accident_Index, Vehicle_Reference, Vehicle_Type,
  Vehicle_Manoeuvre, Skidding_and_Overturning, Hit_Object_in_Carriageway,
  Vehicle_Leaving_Carriageway, Hit_Object_off_Carriageway, Journey_Purpose_of_Driver,
  Sex_of_Driver, Age_of_Driver, Age_Band_of_Driver, Age_of_Vehicle
FROM b_vehicles

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Saving the refined data as delta tables
-- MAGIC Now that we have our refined tables in memory including the new fields, we'll write these back out refined delta tables to add a bit of permanence. It's a good idea to think of these tables as being useful for other purposes too - such as exporting to BI tools, shipping for ML etc. This is the first tier of data we will likely be reporting on in the future.

-- COMMAND ----------

DROP TABLE IF EXISTS s_accidents;

CREATE TABLE s_accidents
USING delta
PARTITIONED BY (la_highway)
SELECT *
FROM accident_summary

-- COMMAND ----------

OPTIMIZE s_accidents ZORDER BY (la_district)

-- COMMAND ----------

DROP TABLE IF EXISTS s_casualties;

CREATE TABLE s_casualties
USING delta
PARTITIONED BY (Age_Band_of_Casualty)
SELECT *
FROM casualty_summary

-- COMMAND ----------

OPTIMIZE s_casualties ZORDER BY (Casualty_Type)

-- COMMAND ----------

DROP TABLE IF EXISTS s_vehicles;

CREATE TABLE s_vehicles
USING delta
PARTITIONED BY (Age_Band_of_Driver)
SELECT *
FROM vehicle_summary

-- COMMAND ----------

OPTIMIZE s_vehicles ZORDER BY (Vehicle_Type)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Coalesce clean data
-- MAGIC Now let's build some aggregated tables for proper BI reporting

-- COMMAND ----------

SELECT * FROM l_weather

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW mva_raw AS
SELECT a.Accident_Severity, a.Number_of_Casualties, a.la_highway, a.la_district, a.Weather_Conditions,
  c.Casualty_Severity, c.Casualty_Type, c.Casualty_Reference, c.Age_of_Casualty,
  v.Vehicle_Reference, v.Vehicle_Type, v.Vehicle_Manoeuvre, v.Age_of_Driver,
  cs.label as Casualty_Severity_Desc, ct.label as Casualty_Type_Desc,
  vt.label as Vehicle_Type_Desc,
  vm.label as Vehicle_Manoeuvre_Desc, w.label as Weather_Conditions_Desc
FROM s_accidents a 
  JOIN s_casualties c ON a.Accident_Index=c.Accident_Index 
  JOIN s_vehicles v ON a.Accident_Index=v.Accident_Index
  JOIN l_casualty_severity cs ON c.Casualty_Severity=cs.code
  JOIN l_casualty_type ct ON ct.code=c.Casualty_Type
  JOIN l_vehicle_type vt ON v.Vehicle_Type=vt.code
  JOIN l_vehicle_manoeuvre vm ON v.Vehicle_Manoeuvre=vm.code
  JOIN l_weather w ON w.code=a.Weather_Conditions

-- COMMAND ----------

SELECT * from mva_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Finally, now that we have a view we're happy with, let's write that out to our final table, also referred to as a 'gold' table.

-- COMMAND ----------

DROP TABLE IF EXISTS g_mva;

CREATE TABLE g_mva
USING delta
PARTITIONED BY (Casualty_Severity)
SELECT *
FROM mva_raw

-- COMMAND ----------

OPTIMIZE g_mva ZORDER BY (Vehicle_Type)
