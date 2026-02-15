-- Drug mapping: join source NDC to concept.concept_code to get source concept_id, then map to standard
-- via concept_relationship 'Maps to'. Match order: concept_code (direct), digits-only, 11-pad, LIKE.
-- Source: one row per (drug_ndc_normalized, drug_name) with representative drug_ndc_raw for normalization.
CREATE OR REPLACE TABLE stg.map_drug_order AS
WITH
-- One row per (drug_ndc_normalized, drug_name) that has NDC, with representative drug_ndc_raw
source_ndc AS (
  SELECT drug_ndc_raw, drug_ndc_normalized, drug_name
  FROM (
    SELECT
      drug_ndc_raw,
      drug_ndc_normalized,
      TRIM(drug_name) AS drug_name,
      ROW_NUMBER() OVER (PARTITION BY drug_ndc_normalized, TRIM(drug_name) ORDER BY drug_ndc_raw) AS rn
    FROM stg.medication_orders
    WHERE (drug_ndc_normalized IS NOT NULL AND TRIM(drug_ndc_normalized) <> '')
      AND (drug_ndc_raw IS NOT NULL AND TRIM(drug_ndc_raw) <> '')
  ) t
  WHERE rn = 1
),
-- 1) Keep only digits and '*' (strip hyphens, spaces, etc.)
cleaned AS (
  SELECT
    drug_ndc_raw,
    drug_ndc_normalized,
    drug_name,
    regexp_replace(drug_ndc_raw, '[^0-9*]', '', 'g') AS ndc_keep_digits_star
  FROM source_ndc
),
-- 2) Build match keys: ndc_pattern (* -> % for LIKE), ndc_digits (exact), ndc_digits_11 (pad to 11)
keys AS (
  SELECT
    drug_ndc_raw,
    drug_ndc_normalized,
    drug_name,
    ndc_keep_digits_star,
    REPLACE(ndc_keep_digits_star, '*', '%') AS ndc_pattern,
    REPLACE(ndc_keep_digits_star, '*', '') AS ndc_digits,
    CASE
      WHEN LENGTH(REPLACE(ndc_keep_digits_star, '*', '')) BETWEEN 1 AND 11
        THEN LPAD(REPLACE(ndc_keep_digits_star, '*', ''), 11, '0')
      ELSE NULL
    END AS ndc_digits_11
  FROM cleaned
),
-- NDC concepts: join on concept_code to get source concept_id, then map to standard
ndc_concepts AS (
  SELECT concept_id, concept_code
  FROM cdm.concept
  WHERE vocabulary_id = 'NDC' AND invalid_reason IS NULL
),
-- 3a) Direct join: source NDC (normalized) to concept.concept_code -> source concept_id
m_concept_code AS (
  SELECT
    k.drug_ndc_raw,
    k.drug_ndc_normalized,
    k.drug_name,
    c.concept_id AS ndc_concept_id,
    c.concept_code,
    0 AS match_rank
  FROM keys k
  JOIN ndc_concepts c ON c.concept_code = k.drug_ndc_normalized
  WHERE k.drug_ndc_normalized IS NOT NULL AND TRIM(k.drug_ndc_normalized) <> ''
),
-- 3b) Exact match on digits-only (from raw)
m_exact AS (
  SELECT
    k.drug_ndc_raw,
    k.drug_ndc_normalized,
    k.drug_name,
    c.concept_id AS ndc_concept_id,
    c.concept_code,
    1 AS match_rank
  FROM keys k
  JOIN ndc_concepts c ON c.concept_code = k.ndc_digits
  WHERE k.ndc_digits IS NOT NULL AND k.ndc_digits <> ''
),
-- 3c) Exact match on 11-digit padded
m_exact11 AS (
  SELECT
    k.drug_ndc_raw,
    k.drug_ndc_normalized,
    k.drug_name,
    c.concept_id AS ndc_concept_id,
    c.concept_code,
    2 AS match_rank
  FROM keys k
  JOIN ndc_concepts c ON c.concept_code = k.ndc_digits_11
  WHERE k.ndc_digits_11 IS NOT NULL
),
-- 3d) Wildcard match using LIKE (handles e.g. 54569-3335-*0); skip pattern that is only '%'
m_like AS (
  SELECT
    k.drug_ndc_raw,
    k.drug_ndc_normalized,
    k.drug_name,
    c.concept_id AS ndc_concept_id,
    c.concept_code,
    3 AS match_rank
  FROM keys k
  JOIN ndc_concepts c ON c.concept_code LIKE k.ndc_pattern
  WHERE k.ndc_pattern IS NOT NULL AND k.ndc_pattern <> '' AND k.ndc_pattern <> '%'
),
all_matches AS (
  SELECT * FROM m_concept_code
  UNION ALL
  SELECT * FROM m_exact
  UNION ALL
  SELECT * FROM m_exact11
  UNION ALL
  SELECT * FROM m_like
),
-- 4) Best match per drug_ndc_raw: exact > exact_11pad > like, then longest concept_code
best AS (
  SELECT drug_ndc_raw, drug_ndc_normalized, drug_name, ndc_concept_id, concept_code
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY drug_ndc_raw
        ORDER BY match_rank ASC, LENGTH(concept_code) DESC, concept_code ASC
      ) AS rn
    FROM all_matches
  ) x
  WHERE rn = 1
),
-- Standard concept_id via concept_relationship 'Maps to' (source concept_id -> standard concept_id)
ndc_to_standard AS (
  SELECT
    cr.concept_id_1 AS ndc_concept_id,
    cr.concept_id_2 AS standard_concept_id
  FROM cdm.concept_relationship cr
  WHERE cr.relationship_id = 'Maps to' AND cr.invalid_reason IS NULL
),
mapped AS (
  SELECT
    k.drug_ndc_normalized,
    k.drug_name,
    COALESCE(t.concept_id, 0) AS drug_concept_id,
    COALESCE(b.ndc_concept_id, 0) AS drug_source_concept_id
  FROM keys k
  LEFT JOIN best b ON b.drug_ndc_raw = k.drug_ndc_raw
  LEFT JOIN ndc_to_standard std ON std.ndc_concept_id = b.ndc_concept_id
  LEFT JOIN cdm.concept t ON t.concept_id = std.standard_concept_id AND t.standard_concept = 'S'
)
-- One row per (drug_ndc_normalized, drug_name); prefer row with non-zero concept ids
SELECT drug_ndc_normalized, drug_name, drug_concept_id, drug_source_concept_id
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY drug_ndc_normalized, drug_name
      ORDER BY (CASE WHEN drug_source_concept_id <> 0 THEN 0 ELSE 1 END), (CASE WHEN drug_concept_id <> 0 THEN 0 ELSE 1 END)
    ) AS rn
  FROM mapped
) x
WHERE rn = 1;

-- Rows with only drug_name (no NDC): add with concept_id from drug_name_to_concept (Hecate) or 0
INSERT INTO stg.map_drug_order
SELECT DISTINCT
  NULL,
  TRIM(mo.drug_name),
  COALESCE(dnc.concept_id, 0),
  0
FROM stg.medication_orders mo
LEFT JOIN stg.drug_name_to_concept dnc ON dnc.drug_name = TRIM(mo.drug_name)
WHERE (mo.drug_ndc_normalized IS NULL OR TRIM(mo.drug_ndc_normalized) = '')
  AND mo.drug_name IS NOT NULL AND TRIM(mo.drug_name) <> ''
  AND NOT EXISTS (SELECT 1 FROM stg.map_drug_order d WHERE d.drug_name = TRIM(mo.drug_name) AND d.drug_ndc_normalized IS NULL);

-- Apply custom NDC overrides (e.g. when NDC is not in vocabulary or maps to wrong concept)
UPDATE stg.map_drug_order
SET drug_concept_id = c.drug_concept_id, drug_source_concept_id = 0
FROM stg.custom_ndc_mapping c
WHERE stg.map_drug_order.drug_ndc_normalized = c.drug_ndc_normalized;
