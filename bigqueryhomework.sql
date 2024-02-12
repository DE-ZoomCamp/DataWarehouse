-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://homeworkde/2022/green_tripdata_2022-*.parquet']
);


CREATE OR REPLACE TABLE `ny_taxi.green_tripdata_nonpartitioned`
AS SELECT * FROM `ny_taxi.external_green_tripdata`;


CREATE OR REPLACE TABLE `ny_taxi.green_tripdata_partitioned`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS (
  SELECT * FROM `ny_taxi.external_green_tripdata`
);



SELECT COUNT(DISTINCT(PULocationID)) FROM `ny_taxi.external_green_tripdata`;
SELECT COUNT(DISTINCT(PULocationID)) FROM `ny_taxi.green_tripdata_nonpartitioned`;

SELECT COUNT(*) FROM `ny_taxi.external_green_tripdata` WHERE fare_amount = 0;

SELECT DISTINCT(PULocationID) FROM  `ny_taxi.green_tripdata_nonpartitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

SELECT DISTINCT(PULocationID) FROM  `ny_taxi.green_tripdata_partitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
