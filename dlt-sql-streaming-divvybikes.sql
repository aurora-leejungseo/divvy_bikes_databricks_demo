-- Databricks notebook source
-- DBTITLE 1,Divvy Bikes Data Pipeline (Main)
-- MAGIC %md
-- MAGIC SQL-based Delta Live Tables script leveraging Auto Loader to handle streaming (and batch) data.

-- COMMAND ----------

-- DBTITLE 1,Global Settings
-- Increase the default # files used to infer schema as rain & snow do not occur frequently
SET spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles = 100000

-- COMMAND ----------

-- DBTITLE 1,Create Raw Station Status - Bronze Table - Auto Loader & DLT SQL
-- Create the bronze station status table containing the raw JSON
CREATE STREAMING LIVE TABLE raw_station_status
COMMENT "The raw station status data, ingested from /FileStore/DivvyBikes/api_response/station_status."
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT * FROM cloud_files("/FileStore/DivvyBikes/api_response/station_status", "json", map("cloudFiles.inferColumnTypes", "true"));

-- STREAMING LIVE TABLE: a delta table (default data table format in Azure Databricks) with extra support for streaming or incremental data processing, in here, since the data is continuously arriving, we used streaming live table, which processes newly arrived data incrementally

-- cloud files: passing the location of the files in cloud stroage and the expected format, which is JSON. This invokes Databricks auto loader, which is an optimized streaming pattern to efficiently ingest data into the lake house without triggers or manual scheduling

-- COMMAND ----------

-- DBTITLE 1,Create Cleaned Station Status - Silver Table - DLT SQL
-- Create the silver station status table by exploding on station and picking the desired fields.
CREATE STREAMING LIVE TABLE cleaned_station_status (
  CONSTRAINT valid_station_id EXPECT (station_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT over_24hr_old_data EXPECT (secs_since_last_reported < 86400)
  )
PARTITIONED BY (last_updated_date)
COMMENT "The cleaned station status data with valid station_ids and partitioned by station id."
TBLPROPERTIES ("quality" = "silver")
AS
SELECT 
  stations.station_id,
  stations.num_bikes_available,
  stations.num_bikes_disabled,
  stations.num_docks_available,
  stations.num_docks_disabled,
  stations.num_ebikes_available,
  stations.station_status,
  stations.is_renting,
  stations.is_returning,
  stations.last_reported,
  CAST(stations.last_reported AS timestamp) AS last_reported_ts,
  last_updated,
  last_updated_ts,
  date(last_updated_ts) AS last_updated_date,
  unix_timestamp(date_trunc('HOUR', last_updated_ts))  AS last_updated_hr,
  last_updated - stations.last_reported AS secs_since_last_reported
FROM (
  SELECT 
    EXPLODE(data.stations) AS stations,
    last_updated, 
    CAST(last_updated AS timestamp) AS last_updated_ts
  FROM STREAM(LIVE.raw_station_status)
  );


-- Constraint: to keep high quality data, define a quality constraint
-- STEAM(LIVE.raw_station_status): previous streaming live table
-- EXPLODE(data.stations): explodes the JSON into multiple rows, and prepares the columns exactly how we want by SELECT() 

-- COMMAND ----------

-- DBTITLE 1,Create Raw Weather Information - Bronze Table - Auto Loader & DLT SQL
-- Create the bronze weather info table containing the raw JSON
CREATE STREAMING LIVE TABLE raw_weather_information
PARTITIONED BY (dt_date)
COMMENT "The raw weather data, ingested from /FileStore/DivvyBikes/api_response/weather_info."
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT 
  *,
  date(CAST(dt AS timestamp)) AS dt_date,
  regexp_extract(input_file_name(),"(.*)_(.*).json", 2) AS station_id,
  input_file_name() AS json_file_name
FROM cloud_files("/FileStore/DivvyBikes/api_response/weather_info", "json", map("cloudFiles.inferColumnTypes", "true", "cloudFiles.useIncrementalListing", "false"));

-- autoloader: automatically infers the schema of any JSON payload, so don't have to define it in advance
-- map: it detects changes in the schema over time, and captures any unexpected data in a rescue data column, then restarts the pipeline for the user, so the user can start using the new data right away. 


-- COMMAND ----------

-- DBTITLE 1,Create Cleaned Weather Information - Silver Table - DLT SQL
-- Create the silver weather info table.
CREATE STREAMING LIVE TABLE cleaned_weather_information (
CONSTRAINT valid_station_id EXPECT (station_id IS NOT NULL) 
ON VIOLATION DROP ROW
)
PARTITIONED BY (dt_date)
COMMENT "The cleaned weather info data with valid station_ids 
and partitioned by station id."
TBLPROPERTIES ("quality" = "silver")
AS
SELECT 
  weather.main AS weather_main,
  weather.description AS weather_description,
  station_id AS station_id,
  coord.lat AS coord_lat,
  coord.lon AS coord_lon,
  base AS base,
  main.temp AS main_temp,
  main.feels_like AS main_feels_like,
  main.temp_min AS main_temp_min,
  main.temp_max AS main_temp_max,
  main.pressure AS main_pressure,
  main.humidity AS main_humidity,
  visibility AS visibility,
  wind.speed AS wind_speed,
  wind.deg AS wind_deg,
  wind.gust AS wind_gust,
  clouds.all AS clouds_add,
  nvl(snow.1h,0) AS snow_1h,
  nvl(rain.1h,0) AS rain_1h,
  json_file_name AS json_file_name,
  dt AS dt,
  CAST(dt AS timestamp) AS dt_ts,
  dt_date AS dt_date,
  unix_timestamp(date_trunc('HOUR', CAST(dt AS timestamp)))  AS dt_hr,
  sys.type AS sys_type,
  sys.id AS sys_id,
  sys.country AS sys_country,
  sys.sunrise AS sys_sunrise,
  sys.sunset AS sys_sunset,
  timezone AS timezone,
  id AS id,
  name AS name,
  cod AS cod
FROM STREAM(LIVE.raw_weather_information);

-- COMMAND ----------

-- DBTITLE 1,Create Raw Station Information - Bronze Table - Auto Loader & DLT SQL
-- Create the bronze station information table containing the raw JSON
CREATE STREAMING LIVE TABLE raw_station_information
COMMENT "The raw station information data, ingested from /FileStore/DivvyBikes/api_response/station_information."
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT * FROM cloud_files("/FileStore/DivvyBikes/api_response/station_information", "json", map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

-- DBTITLE 1,Create Exploded Raw Station Information - Temp Bronze Table - DLT SQL
-- Create temporary bronze station information table containing the exploded raw JSON and picking the desired fields.
CREATE TEMPORARY STREAMING LIVE TABLE exploded_raw_station_information
AS 
SELECT 
  stations.station_id,
  stations.external_id,
  stations.name,
  stations.short_name,
  stations.station_type,
  stations.has_kiosk,
  stations.capacity,
  stations.rack_model,      
  stations.eightd_has_key_dispenser,
  stations.electric_bike_surcharge_waiver,
  stations.lat,
  stations.lon,
  stations.rental_methods,
  stations.rental_uris,
  last_updated,
  last_updated_ts
FROM (
SELECT 
  EXPLODE(data.stations) AS stations,
  last_updated, 
  CAST(last_updated AS timestamp) AS last_updated_ts
FROM STREAM(LIVE.raw_station_information)
);

-- CREATE TEMPORARY STREAMING LIVE TABLE: exploding the JSON structure into a temporary live table

-- COMMAND ----------

-- DBTITLE 1,Create Cleaned Station Information - Silver Table - DLT SQL
-- Create the silver station information table.
CREATE STREAMING LIVE TABLE cleaned_station_information
COMMENT "The cleaned station information data."
TBLPROPERTIES ("quality" = "silver"); 

-- COMMAND ----------

-- DBTITLE 1,Merge (Upsert) Cleaned Station Information - Silver Table - DLT SQL
-- Upsert/merge new stations into silver station information table.
APPLY CHANGES INTO LIVE.cleaned_station_information FROM STREAM(LIVE.exploded_raw_station_information)
  KEYS (station_id)
  SEQUENCE BY last_updated;

-- APPLY CHANGES INTO: apply changes into syntax to merge or upsest records into the silver level table, either overwrite an existing station or inserts infomraiton about new stations that appear in the data based on the station_id. 