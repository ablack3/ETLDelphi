-- Load custom concept mappings from CSV (source_value, domain, concept_id).
-- Use when vocabulary mapping returns 0. Run export_top_unmapped_source_values, add concept_id column, save.
-- Path from config custom_mapping_path; empty = use package default or empty table.
CREATE OR REPLACE TABLE stg.custom_concept_mapping (
    source_value VARCHAR,
    domain VARCHAR,
    concept_id INTEGER
);
INSERT INTO stg.custom_concept_mapping
SELECT TRIM(source_value), TRIM(domain), concept_id
FROM read_csv('@customMappingPath', header = true, auto_detect = true)
WHERE concept_id IS NOT NULL AND concept_id > 0;
