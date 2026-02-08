-- Optional: map care_site from Clinic_ID/Clinic_Code. For v1 we create a minimal map from encounter clinic_id.
-- If no clinic data, table will be empty or single row; care_site_id can be null in visit_occurrence.
CREATE OR REPLACE TABLE stg.map_care_site AS
SELECT
    care_site_key,
    ROW_NUMBER() OVER (ORDER BY care_site_key) AS care_site_id
FROM (
    SELECT DISTINCT COALESCE(TRIM(clinic_id), 'UNKNOWN') AS care_site_key
    FROM stg.encounter
    WHERE clinic_id IS NOT NULL AND TRIM(clinic_id) <> ''
) t;
