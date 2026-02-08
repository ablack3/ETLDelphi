-- Create staging schema if not exists.
-- ETL uses: src (existing), stg (staging), cdm (existing OMOP).
CREATE SCHEMA IF NOT EXISTS stg;
