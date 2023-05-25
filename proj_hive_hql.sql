CREATE DATABASE IF NOT EXISTS green_taxi_staging;

USE green_taxi_staging;

CREATE EXTERNAL TABLE IF NOT EXISTS green_taxi_2020(
VendorID BIGINT, lpep_pickup_datetime BIGINT, lpep_dropoff_datetime BIGINT,
store_and_fwd_flag STRING, RatecodeID DOUBLE, PULocationID BIGINT,
DOLocationID BIGINT, passenger_count DOUBLE, trip_distance DOUBLE,
fare_amount DOUBLE, extra DOUBLE, mta_tax DOUBLE, tip_amount DOUBLE,
tolls_amount DOUBLE, ehail_fee DOUBLE, improvement_surcharge DOUBLE,
total_amount DOUBLE, payment_type DOUBLE, trip_type DOUBLE,
congestion_surcharge DOUBLE) 
STORED AS PARQUET
LOCATION '/user/fatir/hive-table'
tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH 'taxi-data/*.parquet'
OVERWRITE INTO TABLE green_taxi_2020;