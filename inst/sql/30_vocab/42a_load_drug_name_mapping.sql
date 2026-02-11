-- Load drug name -> concept_id lookup from Hecate-built CSV (fallback when NDC is missing).
-- Run build_drug_name_mapping.R first to populate the CSV. Default inst/extdata/drug_name_to_concept.csv.
CREATE OR REPLACE TABLE stg.drug_name_to_concept (
    drug_name VARCHAR,
    concept_id INTEGER
);
INSERT INTO stg.drug_name_to_concept
SELECT drug_name, concept_id FROM read_csv('@drugNameMappingPath', header = true, auto_detect = true);
