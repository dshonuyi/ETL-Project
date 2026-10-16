-- Map each supplier ZIP to its nearest NOAA weather station and pull daily high temperatures.
-- {stations_fqn}, {timeseries_fqn}, {date_col}, {high_col} are filled in by etl_pipeline.py.

-- Supplier ZIPs normalized to 5 digits (handles ZIP+4 and messy strings)
CREATE OR REPLACE VIEW vw_supplier_zips AS
SELECT DISTINCT
    LPAD(SUBSTR(REGEXP_REPLACE(TRIM("postalpostalcode"), '[^0-9]', ''), 1, 5), 5, '0') AS zip
FROM supplier_case
WHERE "postalpostalcode" IS NOT NULL
  AND REGEXP_REPLACE(TRIM("postalpostalcode"), '[^0-9]', '') <> '';

-- ZIP centroids from the Census ZCTA file
CREATE OR REPLACE VIEW vw_zip_latlon AS
SELECT
    LPAD(TRIM(zcta5), 5, '0')    AS zip,
    TRY_TO_DOUBLE(centroid_lat)  AS zip_lat,
    TRY_TO_DOUBLE(centroid_lon)  AS zip_lon
FROM zcta_2021
WHERE zcta5 IS NOT NULL;

-- Nearest station per ZIP (haversine distance, km)
CREATE OR REPLACE VIEW vw_zip_station_nearest AS
WITH cand AS (
  SELECT
    zl.zip,
    si.noaa_weather_station_id,
    2 * 6371 * ASIN(SQRT(
      POWER(SIN(RADIANS((zl.zip_lat - TRY_TO_DOUBLE(si.latitude)) / 2)), 2) +
      COS(RADIANS(zl.zip_lat)) * COS(RADIANS(TRY_TO_DOUBLE(si.latitude))) *
      POWER(SIN(RADIANS((zl.zip_lon - TRY_TO_DOUBLE(si.longitude)) / 2)), 2)
    )) AS dist_km
  FROM vw_supplier_zips sz
  JOIN vw_zip_latlon zl ON zl.zip = sz.zip
  JOIN {stations_fqn} si
    ON si.latitude IS NOT NULL AND si.longitude IS NOT NULL
)
SELECT zip, noaa_weather_station_id
FROM cand
QUALIFY ROW_NUMBER() OVER (PARTITION BY zip ORDER BY dist_km) = 1;

-- Daily high temperature for every supplier ZIP
CREATE OR REPLACE TABLE supplier_zip_code_weather AS
SELECT
    z.zip,
    ts.{date_col}::DATE AS wx_date,
    ts.{high_col}       AS high_temp
FROM vw_zip_station_nearest z
JOIN {timeseries_fqn} ts
  ON ts.noaa_weather_station_id = z.noaa_weather_station_id
WHERE ts.{high_col} IS NOT NULL;
