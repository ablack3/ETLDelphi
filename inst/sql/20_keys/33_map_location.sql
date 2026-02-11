-- Deterministic location_id from distinct address (concat address_1, address_2, city, state, zip).
-- Source: stg.enrollment address fields.
CREATE OR REPLACE TABLE stg.map_location AS
WITH loc_keys AS (
    SELECT DISTINCT
        COALESCE(TRIM(address_line_1), '') AS a1,
        COALESCE(TRIM(address_line_2), '') AS a2,
        COALESCE(TRIM(city), '') AS city,
        COALESCE(TRIM(state), '') AS state,
        COALESCE(TRIM(zip_code), '') AS zip_code
    FROM stg.enrollment
    WHERE (address_line_1 IS NOT NULL AND TRIM(address_line_1) <> '')
       OR (city IS NOT NULL AND TRIM(city) <> '')
       OR (state IS NOT NULL AND TRIM(state) <> '')
       OR (zip_code IS NOT NULL AND TRIM(zip_code) <> '')
)
SELECT
    a1 || '|' || a2 || '|' || city || '|' || state || '|' || zip_code AS location_key,
    ROW_NUMBER() OVER (ORDER BY a1, a2, city, state, zip_code) AS location_id
FROM loc_keys;
