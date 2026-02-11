-- Load cdm.location from stg.map_location. Parse location_key (a1|a2|city|state|zip) for address fields.
-- One row per location_id; address from first matching enrollment.
INSERT INTO cdm.location (location_id, address_1, address_2, city, state, zip, location_source_value)
WITH enr_loc AS (
    SELECT
        TRIM(address_line_1) || '|' || COALESCE(TRIM(address_line_2), '') || '|' || COALESCE(TRIM(city), '') || '|' || COALESCE(TRIM(state), '') || '|' || COALESCE(TRIM(zip_code), '') AS location_key,
        TRIM(address_line_1) AS address_1,
        TRIM(COALESCE(address_line_2, '')) AS address_2,
        TRIM(city) AS city,
        TRIM(state) AS state,
        TRIM(zip_code) AS zip_code,
        ROW_NUMBER() OVER (PARTITION BY (TRIM(address_line_1) || '|' || COALESCE(TRIM(address_line_2), '') || '|' || COALESCE(TRIM(city), '') || '|' || COALESCE(TRIM(state), '') || '|' || COALESCE(TRIM(zip_code), '')) ORDER BY member_id) AS rn
    FROM stg.enrollment
    WHERE (address_line_1 IS NOT NULL AND TRIM(address_line_1) <> '') OR (city IS NOT NULL AND TRIM(city) <> '') OR (state IS NOT NULL AND TRIM(state) <> '') OR (zip_code IS NOT NULL AND TRIM(zip_code) <> '')
)
SELECT
    m.location_id,
    SUBSTR(e.address_1, 1, 50),
    CASE WHEN e.address_2 <> '' THEN SUBSTR(e.address_2, 1, 50) ELSE NULL END,
    CASE WHEN e.city <> '' THEN SUBSTR(e.city, 1, 50) ELSE NULL END,
    CASE WHEN e.state <> '' THEN SUBSTR(e.state, 1, 2) ELSE NULL END,
    CASE WHEN e.zip_code <> '' THEN SUBSTR(e.zip_code, 1, 9) ELSE NULL END,
    m.location_key
FROM stg.map_location m
LEFT JOIN (SELECT * FROM enr_loc WHERE rn = 1) e ON e.location_key = m.location_key
WHERE NOT EXISTS (SELECT 1 FROM cdm.location c WHERE c.location_id = m.location_id);
