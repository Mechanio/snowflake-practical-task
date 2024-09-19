-- Set Up the Environment
CREATE DATABASE nyc_airbnb;

CREATE WAREHOUSE nyc_warehouse WITH 
WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 300
AUTO_RESUME = TRUE;

USE WAREHOUSE nyc_warehouse;

CREATE STAGE data DIRECTORY = ( ENABLE = true );

CREATE TABLE raw_airbnb_data (
  id INT,
  name STRING,
  host_id INT,
  host_name STRING,
  neighbourhood_group STRING,
  neighbourhood STRING,
  latitude FLOAT,
  longitude FLOAT,
  room_type STRING,
  price FLOAT,
  minimum_nights INT,
  number_of_reviews INT,
  last_review DATE,
  reviews_per_month FLOAT,
  calculated_host_listings_count INT,
  availability_365 INT
);

CREATE TABLE transformed_data (
  id INT,
  name STRING,
  host_id INT,
  host_name STRING,
  neighbourhood_group STRING,
  neighbourhood STRING,
  latitude FLOAT,
  longitude FLOAT,
  room_type STRING,
  price FLOAT,
  minimum_nights INT,
  number_of_reviews INT,
  last_review DATE,
  reviews_per_month FLOAT,
  calculated_host_listings_count INT,
  availability_365 INT
);

-- create pipeline
CREATE PIPE nyc_pipe
AS COPY INTO raw_airbnb_data
FROM @data
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

-- create file format specifications
CREATE FILE FORMAT my_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n';

--load data from stage to table
COPY INTO raw_airbnb_data from @data file_format=my_csv_format;

-- data transformation
INSERT INTO transformed_data
SELECT
    id, name, host_id, host_name, neighbourhood_group, neighbourhood,
    latitude, longitude, room_type, price, minimum_nights, number_of_reviews,
    COALESCE(TRY_CAST(last_review AS DATE),(SELECT MIN(last_review) FROM raw_airbnb_data) AS last_review,
    COALESCE(reviews_per_month, 0) AS reviews_per_month,
    calculated_host_listings_count, availability_365
FROM RAW_AIRBNB_DATA
WHERE price > 0
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL;

-- data quality check
SELECT COUNT(*) FROM transformed_data
WHERE price IS NULL OR minimum_nights IS NULL OR availability_365 IS NULL;

-- creating task
CREATE TASK daily_airbnb_task
    WAREHOUSE = nyc_warehouse
    SCHEDULE = 'USING CRON 0 0 * * * UTC'
AS
    INSERT INTO transformed_data
    SELECT
        id, name, host_id, host_name, neighbourhood_group, neighbourhood,
        latitude, longitude, room_type, price, minimum_nights, number_of_reviews,
        COALESCE(TRY_CAST(last_review AS DATE),(SELECT MIN(last_review) FROM raw_airbnb_data)) AS last_review,
        COALESCE(reviews_per_month, 0) AS reviews_per_month,
        calculated_host_listings_count, availability_365
    FROM RAW_AIRBNB_DATA
    WHERE price > 0
      AND latitude IS NOT NULL
      AND longitude IS NOT NULL;

ALTER TASK transform_airbnb_data_daily RESUME;

CREATE STREAM raw_airbnb_data_stream ON TABLE raw_airbnb_data;

CREATE STREAM transformed_data_stream ON TABLE transformed_data;
