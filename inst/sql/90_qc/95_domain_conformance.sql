-- QC: domain conformance - % of mapped records (concept_id != 0) where concept.domain_id matches the CDM table.
-- After domain routing (70_domain_routing.sql), expect 100% conformance.
CREATE TABLE IF NOT EXISTS stg.qc_domain_conformance (
    cdm_table VARCHAR(64),
    expected_domain VARCHAR(20),
    concept_domain VARCHAR(20),
    record_count INTEGER,
    total_mapped INTEGER,
    conformance_pct DOUBLE
);

DELETE FROM stg.qc_domain_conformance WHERE 1=1;

-- condition_occurrence
INSERT INTO stg.qc_domain_conformance (cdm_table, expected_domain, concept_domain, record_count, total_mapped, conformance_pct)
SELECT 'condition_occurrence', 'Condition', c.domain_id, COUNT(*),
       SUM(COUNT(*)) OVER (), 100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0)
FROM cdm.condition_occurrence co
JOIN cdm.concept c ON c.concept_id = co.condition_concept_id
WHERE co.condition_concept_id <> 0
GROUP BY c.domain_id;

-- drug_exposure
INSERT INTO stg.qc_domain_conformance (cdm_table, expected_domain, concept_domain, record_count, total_mapped, conformance_pct)
SELECT 'drug_exposure', 'Drug', c.domain_id, COUNT(*),
       SUM(COUNT(*)) OVER (), 100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0)
FROM cdm.drug_exposure de
JOIN cdm.concept c ON c.concept_id = de.drug_concept_id
WHERE de.drug_concept_id <> 0
GROUP BY c.domain_id;

-- measurement
INSERT INTO stg.qc_domain_conformance (cdm_table, expected_domain, concept_domain, record_count, total_mapped, conformance_pct)
SELECT 'measurement', 'Measurement', c.domain_id, COUNT(*),
       SUM(COUNT(*)) OVER (), 100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0)
FROM cdm.measurement m
JOIN cdm.concept c ON c.concept_id = m.measurement_concept_id
WHERE m.measurement_concept_id <> 0
GROUP BY c.domain_id;

-- observation
INSERT INTO stg.qc_domain_conformance (cdm_table, expected_domain, concept_domain, record_count, total_mapped, conformance_pct)
SELECT 'observation', 'Observation', c.domain_id, COUNT(*),
       SUM(COUNT(*)) OVER (), 100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0)
FROM cdm.observation o
JOIN cdm.concept c ON c.concept_id = o.observation_concept_id
WHERE o.observation_concept_id <> 0
GROUP BY c.domain_id;

-- procedure_occurrence
INSERT INTO stg.qc_domain_conformance (cdm_table, expected_domain, concept_domain, record_count, total_mapped, conformance_pct)
SELECT 'procedure_occurrence', 'Procedure', c.domain_id, COUNT(*),
       SUM(COUNT(*)) OVER (), 100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0)
FROM cdm.procedure_occurrence po
JOIN cdm.concept c ON c.concept_id = po.procedure_concept_id
WHERE po.procedure_concept_id <> 0
GROUP BY c.domain_id;
