-- Race mapping: minimal mapping. OMOP: 8527 = white, 8516 = black, 8552 = native, 8657 = asian, 38003563 = hispanic, 0 = unmapped.
-- Always preserve race_source_value in person. Unmapped -> 0.
CREATE OR REPLACE TABLE stg.map_race AS
SELECT race_source_value, race_concept_id FROM (VALUES
    ('white', 8527),
    ('black', 8516),
    ('black or african american', 8516),
    ('asian', 8657),
    ('american indian or alaska native', 8552),
    ('native hawaiian or other pacific islander', 8552),
    ('hispanic', 38003563),
    ('other', 0),
    ('', 0),
    (NULL, 0)
) AS t(race_source_value, race_concept_id);

-- Add distinct race_clean from enrollment not already mapped -> 0
INSERT INTO stg.map_race
SELECT DISTINCT e.race_clean, 0
FROM stg.enrollment e
WHERE e.race_clean IS NOT NULL AND TRIM(e.race_clean) <> ''
  AND NOT EXISTS (SELECT 1 FROM stg.map_race r WHERE r.race_source_value = e.race_clean);
