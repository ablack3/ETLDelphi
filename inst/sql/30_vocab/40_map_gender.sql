-- Gender mapping: source value -> gender_concept_id (OMOP standard).
-- 8532 = female, 8507 = male, 8551 = unknown. Unmapped -> 0 per config.
CREATE OR REPLACE TABLE stg.map_gender AS
SELECT gender_source_value, gender_concept_id FROM (VALUES
    ('female', 8532),
    ('f', 8532),
    ('male', 8507),
    ('m', 8507),
    ('unknown', 8551),
    ('u', 8551),
    ('other', 8551),
    ('', 0),
    (NULL, 0)
) AS t(gender_source_value, gender_concept_id);

-- Add any distinct values from enrollment that we want to default to 0
INSERT INTO stg.map_gender
SELECT DISTINCT e.gender_clean, 0
FROM stg.enrollment e
WHERE e.gender_clean IS NOT NULL AND TRIM(e.gender_clean) <> ''
  AND NOT EXISTS (SELECT 1 FROM stg.map_gender g WHERE g.gender_source_value = e.gender_clean);
