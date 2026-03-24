
-- Crime Reporting Analytics Database
-- Source files: NIBRSPublicView2020.csv ... NIBRSPublicView2026.csv

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS btree_gin;

CREATE SCHEMA IF NOT EXISTS raw_crime;
CREATE SCHEMA IF NOT EXISTS stg_crime;
CREATE SCHEMA IF NOT EXISTS mart_crime;
CREATE SCHEMA IF NOT EXISTS qa_crime;

-- Example raw table template; repeat for 2020-2026
DROP TABLE IF EXISTS raw_crime.nibrs_2020 CASCADE;
CREATE TABLE raw_crime.nibrs_2020 (
  incident_text        TEXT,
  occurrence_date_text TEXT,
  occurrence_hour_text TEXT,
  nibrs_class_text     TEXT,
  nibrs_desc_text      TEXT,
  offense_count_text   TEXT,
  beat_text            TEXT,
  premise_text         TEXT,
  street_no_text       TEXT,
  street_name_text     TEXT,
  street_type_text     TEXT,
  street_suffix_text   TEXT,
  city_text            TEXT,
  zip_code_text        TEXT,
  longitude_text       TEXT,
  latitude_text        TEXT,
  source_file_year     INT,
  source_file_name     TEXT,
  load_ts              TIMESTAMP DEFAULT now()
);

DROP TABLE IF EXISTS stg_crime.stg_nibrs_offense CASCADE;
CREATE TABLE stg_crime.stg_nibrs_offense (
  stg_row_id           BIGSERIAL PRIMARY KEY,
  incident_id          BIGINT,
  occurrence_date      DATE,
  occurrence_hour      SMALLINT,
  occurrence_ts        TIMESTAMP,
  nibrs_class          TEXT,
  nibrs_description    TEXT,
  offense_count        INTEGER,
  beat_code            TEXT,
  premise_desc         TEXT,
  street_number        TEXT,
  street_name          TEXT,
  street_type          TEXT,
  street_suffix        TEXT,
  full_address         TEXT,
  city_name            TEXT,
  zip_code             TEXT,
  map_longitude        DOUBLE PRECISION,
  map_latitude         DOUBLE PRECISION,
  source_year          INTEGER,
  source_file_name     TEXT,
  year_num             INTEGER,
  quarter_num          INTEGER,
  month_num            INTEGER,
  week_num             INTEGER,
  day_num              INTEGER,
  day_of_week_num      INTEGER,
  day_name             TEXT,
  is_weekend           BOOLEAN,
  hour_band            TEXT,
  incident_nk          TEXT,
  location_nk          TEXT,
  row_hash             TEXT,
  load_ts              TIMESTAMP DEFAULT now()
);

DROP TABLE IF EXISTS mart_crime.fact_crime_offense CASCADE;
DROP TABLE IF EXISTS mart_crime.dim_zipcode CASCADE;
DROP TABLE IF EXISTS mart_crime.dim_city CASCADE;
DROP TABLE IF EXISTS mart_crime.dim_beat CASCADE;
DROP TABLE IF EXISTS mart_crime.dim_premise CASCADE;
DROP TABLE IF EXISTS mart_crime.dim_location CASCADE;
DROP TABLE IF EXISTS mart_crime.dim_offense CASCADE;
DROP TABLE IF EXISTS mart_crime.dim_time CASCADE;
DROP TABLE IF EXISTS mart_crime.dim_date CASCADE;

CREATE TABLE mart_crime.dim_date (
  date_key        INTEGER PRIMARY KEY,
  full_date       DATE UNIQUE,
  year_num        INTEGER,
  quarter_num     INTEGER,
  month_num       INTEGER,
  month_name      TEXT,
  week_num        INTEGER,
  day_num         INTEGER,
  day_of_week_num INTEGER,
  day_name        TEXT,
  is_weekend      BOOLEAN
);

CREATE TABLE mart_crime.dim_time (
  time_key        SMALLINT PRIMARY KEY,
  hour_24         SMALLINT UNIQUE,
  hour_label      TEXT,
  hour_band       TEXT
);

CREATE TABLE mart_crime.dim_offense (
  offense_key        BIGSERIAL PRIMARY KEY,
  nibrs_class        TEXT,
  nibrs_description  TEXT,
  crime_group        TEXT,
  severity_tier      TEXT,
  is_violent         BOOLEAN,
  is_property        BOOLEAN,
  is_drug_related    BOOLEAN,
  is_vehicle_related BOOLEAN,
  UNIQUE(nibrs_class, nibrs_description)
);

CREATE TABLE mart_crime.dim_location (
  location_key       BIGSERIAL PRIMARY KEY,
  location_nk        TEXT UNIQUE,
  street_number      TEXT,
  street_name        TEXT,
  street_type        TEXT,
  street_suffix      TEXT,
  full_address       TEXT,
  map_longitude      DOUBLE PRECISION,
  map_latitude       DOUBLE PRECISION,
  geo_valid_flag     BOOLEAN,
  geocode_precision  TEXT
);

CREATE TABLE mart_crime.dim_premise (
  premise_key        BIGSERIAL PRIMARY KEY,
  premise_desc       TEXT UNIQUE,
  premise_group      TEXT
);

CREATE TABLE mart_crime.dim_beat (
  beat_key           BIGSERIAL PRIMARY KEY,
  beat_code          TEXT UNIQUE,
  patrol_division    TEXT
);

CREATE TABLE mart_crime.dim_city (
  city_key           BIGSERIAL PRIMARY KEY,
  city_name          TEXT UNIQUE
);

CREATE TABLE mart_crime.dim_zipcode (
  zipcode_key        BIGSERIAL PRIMARY KEY,
  zip_code           TEXT UNIQUE
);

CREATE TABLE mart_crime.fact_crime_offense (
  fact_crime_key      BIGSERIAL PRIMARY KEY,
  incident_id         BIGINT NOT NULL,
  date_key            INTEGER REFERENCES mart_crime.dim_date(date_key),
  time_key            SMALLINT REFERENCES mart_crime.dim_time(time_key),
  offense_key         BIGINT REFERENCES mart_crime.dim_offense(offense_key),
  location_key        BIGINT REFERENCES mart_crime.dim_location(location_key),
  premise_key         BIGINT REFERENCES mart_crime.dim_premise(premise_key),
  beat_key            BIGINT REFERENCES mart_crime.dim_beat(beat_key),
  city_key            BIGINT REFERENCES mart_crime.dim_city(city_key),
  zipcode_key         BIGINT REFERENCES mart_crime.dim_zipcode(zipcode_key),
  source_year         INTEGER,
  source_file_name    TEXT,
  offense_count       INTEGER NOT NULL,
  incident_count      INTEGER NOT NULL DEFAULT 1,
  violent_crime_flag  BOOLEAN,
  property_crime_flag BOOLEAN,
  drug_crime_flag     BOOLEAN,
  vehicle_crime_flag  BOOLEAN,
  geo_valid_flag      BOOLEAN,
  load_ts             TIMESTAMP DEFAULT now(),
  uq_row UNIQUE (incident_id, date_key, time_key, offense_key, location_key, premise_key, beat_key)
);

CREATE INDEX IF NOT EXISTS idx_fact_crime_date         ON mart_crime.fact_crime_offense(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_crime_time         ON mart_crime.fact_crime_offense(time_key);
CREATE INDEX IF NOT EXISTS idx_fact_crime_offense      ON mart_crime.fact_crime_offense(offense_key);
CREATE INDEX IF NOT EXISTS idx_fact_crime_location     ON mart_crime.fact_crime_offense(location_key);
CREATE INDEX IF NOT EXISTS idx_fact_crime_premise      ON mart_crime.fact_crime_offense(premise_key);
CREATE INDEX IF NOT EXISTS idx_fact_crime_beat         ON mart_crime.fact_crime_offense(beat_key);
CREATE INDEX IF NOT EXISTS idx_fact_crime_city         ON mart_crime.fact_crime_offense(city_key);
CREATE INDEX IF NOT EXISTS idx_fact_crime_zip          ON mart_crime.fact_crime_offense(zipcode_key);
CREATE INDEX IF NOT EXISTS idx_fact_crime_year_date    ON mart_crime.fact_crime_offense(source_year, date_key);
CREATE INDEX IF NOT EXISTS idx_fact_crime_offense_beat ON mart_crime.fact_crime_offense(offense_key, beat_key, date_key);

CREATE OR REPLACE VIEW mart_crime.vw_daily_crime_counts AS
SELECT
  d.full_date,
  o.crime_group,
  COUNT(DISTINCT f.incident_id) AS incidents,
  SUM(f.offense_count) AS offenses
FROM mart_crime.fact_crime_offense f
JOIN mart_crime.dim_date d ON f.date_key = d.date_key
LEFT JOIN mart_crime.dim_offense o ON f.offense_key = o.offense_key
GROUP BY d.full_date, o.crime_group;
