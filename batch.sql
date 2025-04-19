CREATE DATABASE IF NOT EXISTS nyc_taxi;
USE nyc_taxi;

CREATE TABLE IF NOT EXISTS raw_trip_data (
    VendorID INT,
    tpep_pickup_datetime DATETIME,
    tpep_dropoff_datetime DATETIME,
    passenger_count INT,
    trip_distance DOUBLE,
    RatecodeID INT,
    store_and_fwd_flag CHAR(1),
    PULocationID INT,
    DOLocationID INT,
    payment_type INT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    airport_fee DOUBLE,
    cbd_congestion_fee DOUBLE
);

CREATE TABLE fare_summary_by_day (
    trip_date DATE PRIMARY KEY,
    total_fare DECIMAL(10,2),
    total_extra DECIMAL(10,2),
    total_tip DECIMAL(10,2),
    total_tolls DECIMAL(10,2),
    total_airport DECIMAL(10,2),
    total_total DECIMAL(10,2)
);


CREATE TABLE fare_summary_by_day_batch (
    trip_date DATE PRIMARY KEY,
    total_fare DECIMAL(10,2),
    total_extra DECIMAL(10,2),
    total_tip DECIMAL(10,2),
    total_tolls DECIMAL(10,2),
    total_airport DECIMAL(10,2),
    total_total DECIMAL(10,2)
);


-- for batch processing : getting fare metrics / summary from raw_data table
INSERT INTO fare_summary_by_day_batch
SELECT
    DATE(tpep_pickup_datetime) AS trip_date,
    ROUND(SUM(fare_amount), 2) AS total_fare,
    ROUND(SUM(extra), 2) AS total_extra,
    ROUND(SUM(tip_amount), 2) AS total_tip,
    ROUND(SUM(tolls_amount), 2) AS total_tolls,
    ROUND(SUM(airport_fee), 2) AS total_airport,
    ROUND(SUM(total_amount), 2) AS total_total
FROM raw_trip_data
GROUP BY DATE(tpep_pickup_datetime);

CREATE TABLE avg_speed_fare_by_hour (
    start_time DATETIME,
    end_time DATETIME,
    avg_fare DECIMAL(10, 2),
    avg_speed DECIMAL(10, 2)
);

