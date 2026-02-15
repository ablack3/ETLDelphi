-- Load cdm.provider from stg.map_provider and stg.provider. Only provider_id (renumbered) and specialty; leave provider_name, npi, provider_source_value, gender_source_value NULL for privacy.
INSERT INTO cdm.provider (provider_id, provider_name, npi, specialty_concept_id, provider_source_value, specialty_source_value, gender_source_value)
SELECT
    mp.provider_id,
    NULL,
    NULL,
    NULL,
    NULL,
    SUBSTR(p.specialty, 1, 50),
    NULL
FROM stg.map_provider mp
JOIN stg.provider p ON p.provider_id = mp.provider_id_source
WHERE NOT EXISTS (SELECT 1 FROM cdm.provider c WHERE c.provider_id = mp.provider_id);
