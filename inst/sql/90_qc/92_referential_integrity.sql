-- QC: orphan checks - visit_occurrence.person_id not in person; clinical person_id not in person; visit_occurrence_id not in visit_occurrence.
CREATE TABLE IF NOT EXISTS stg.qc_orphans (
    check_name VARCHAR(128),
    table_name VARCHAR(64),
    orphan_id_column VARCHAR(64),
    orphan_count INTEGER
);

DELETE FROM stg.qc_orphans WHERE 1=1;

INSERT INTO stg.qc_orphans (check_name, table_name, orphan_id_column, orphan_count)
SELECT 'visit_occurrence_person', 'visit_occurrence', 'person_id', COUNT(*)
FROM cdm.visit_occurrence v
WHERE NOT EXISTS (SELECT 1 FROM cdm.person p WHERE p.person_id = v.person_id);

INSERT INTO stg.qc_orphans (check_name, table_name, orphan_id_column, orphan_count)
SELECT 'condition_occurrence_person', 'condition_occurrence', 'person_id', COUNT(*)
FROM cdm.condition_occurrence c
WHERE NOT EXISTS (SELECT 1 FROM cdm.person p WHERE p.person_id = c.person_id);

INSERT INTO stg.qc_orphans (check_name, table_name, orphan_id_column, orphan_count)
SELECT 'drug_exposure_person', 'drug_exposure', 'person_id', COUNT(*)
FROM cdm.drug_exposure d
WHERE NOT EXISTS (SELECT 1 FROM cdm.person p WHERE p.person_id = d.person_id);

INSERT INTO stg.qc_orphans (check_name, table_name, orphan_id_column, orphan_count)
SELECT 'measurement_visit', 'measurement', 'visit_occurrence_id', COUNT(*)
FROM cdm.measurement m
WHERE m.visit_occurrence_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM cdm.visit_occurrence v WHERE v.visit_occurrence_id = m.visit_occurrence_id);

INSERT INTO stg.qc_orphans (check_name, table_name, orphan_id_column, orphan_count)
SELECT 'condition_visit', 'condition_occurrence', 'visit_occurrence_id', COUNT(*)
FROM cdm.condition_occurrence c
WHERE c.visit_occurrence_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM cdm.visit_occurrence v WHERE v.visit_occurrence_id = c.visit_occurrence_id);
