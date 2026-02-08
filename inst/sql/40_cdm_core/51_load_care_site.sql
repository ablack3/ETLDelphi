-- Optional: load cdm.care_site from stg.map_care_site. place_of_service_concept_id null for v1.
INSERT INTO cdm.care_site (care_site_id, care_site_name, care_site_source_value)
SELECT care_site_id, care_site_key, care_site_key
FROM stg.map_care_site
WHERE NOT EXISTS (SELECT 1 FROM cdm.care_site c WHERE c.care_site_id = stg.map_care_site.care_site_id);
