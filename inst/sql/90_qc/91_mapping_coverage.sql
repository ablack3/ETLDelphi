-- QC: mapping coverage - % where *_concept_id = 0 and top source values causing unmapped. Store in stg.qc_mapping_coverage.
CREATE TABLE IF NOT EXISTS stg.qc_mapping_coverage (
    table_name VARCHAR(64),
    concept_column VARCHAR(64),
    total_rows INTEGER,
    unmapped_count INTEGER,
    unmapped_pct DOUBLE,
    top_source_values VARCHAR(1024)
);

DELETE FROM stg.qc_mapping_coverage WHERE 1=1;

INSERT INTO stg.qc_mapping_coverage (table_name, concept_column, total_rows, unmapped_count, unmapped_pct, top_source_values)
SELECT 'condition_occurrence', 'condition_concept_id', total, unmapped, 100.0 * unmapped / NULLIF(total, 0), NULL
FROM (SELECT COUNT(*) AS total, SUM(CASE WHEN condition_concept_id = 0 THEN 1 ELSE 0 END) AS unmapped FROM cdm.condition_occurrence) x;

INSERT INTO stg.qc_mapping_coverage (table_name, concept_column, total_rows, unmapped_count, unmapped_pct, top_source_values)
SELECT 'drug_exposure', 'drug_concept_id', total, unmapped, 100.0 * unmapped / NULLIF(total, 0), NULL
FROM (SELECT COUNT(*) AS total, SUM(CASE WHEN drug_concept_id = 0 THEN 1 ELSE 0 END) AS unmapped FROM cdm.drug_exposure) x;

INSERT INTO stg.qc_mapping_coverage (table_name, concept_column, total_rows, unmapped_count, unmapped_pct, top_source_values)
SELECT 'measurement', 'measurement_concept_id', total, unmapped, 100.0 * unmapped / NULLIF(total, 0), NULL
FROM (SELECT COUNT(*) AS total, SUM(CASE WHEN measurement_concept_id = 0 THEN 1 ELSE 0 END) AS unmapped FROM cdm.measurement) x;

INSERT INTO stg.qc_mapping_coverage (table_name, concept_column, total_rows, unmapped_count, unmapped_pct, top_source_values)
SELECT 'procedure_occurrence', 'procedure_concept_id', total, unmapped, 100.0 * unmapped / NULLIF(total, 0), NULL
FROM (SELECT COUNT(*) AS total, SUM(CASE WHEN procedure_concept_id = 0 THEN 1 ELSE 0 END) AS unmapped FROM cdm.procedure_occurrence) x;
