-- Load cdm.provider from stg.map_provider and stg.provider. provider_id from map; name, npi, specialty from stg.provider.
INSERT INTO cdm.provider (provider_id, provider_name, npi, specialty_concept_id, provider_source_value, specialty_source_value, gender_source_value)
SELECT
    mp.provider_id,
    SUBSTR(p.name, 1, 255),
    SUBSTR(p.npi, 1, 20),
    NULL,
    mp.provider_id_source,
    SUBSTR(p.specialty, 1, 50),
    p.sex_clean
FROM stg.map_provider mp
JOIN stg.provider p ON p.provider_id = mp.provider_id_source
WHERE NOT EXISTS (SELECT 1 FROM cdm.provider c WHERE c.provider_id = mp.provider_id);
