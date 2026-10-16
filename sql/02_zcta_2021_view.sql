-- Clean view over the Census 2021 ZCTA Gazetteer: ZIP code centroids and areas.
CREATE OR REPLACE VIEW zcta_2021 AS
SELECT
    GEOID::STRING                                            AS zcta5,
    GEOID::STRING                                            AS geoid,
    ALAND                                                    AS land_area_m2,
    AWATER                                                   AS water_area_m2,
    ALAND_SQMI::FLOAT                                        AS land_area_sqmi,
    AWATER_SQMI::FLOAT                                       AS water_area_sqmi,
    TRY_TO_DOUBLE(NULLIF(TRIM(TO_VARCHAR(INTPTLAT)),  ''))   AS centroid_lat,
    TRY_TO_DOUBLE(NULLIF(TRIM(TO_VARCHAR(INTPTLONG)), ''))   AS centroid_lon,
    (ALAND_SQMI::FLOAT + AWATER_SQMI::FLOAT)                 AS total_area_sqmi
FROM zcta_2021_raw;
