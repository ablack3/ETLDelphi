-- ============================================================================
-- VARIANT_ANNOTATION: GWAS annotation rows for each variant occurrence.
-- Creates multiple annotation rows per variant:
--   1. gwas_beta         → value_as_number (GWAS effect size)
--   2. gwas_pvalue       → value_as_number (GWAS p-value)
--   3. gwas_source       → value_as_string (IEU OpenGWAS ID)
--   4. trait_association  → value_as_string (trait label)
--   5. effect_allele_frequency → value_as_number (EAF)
-- ============================================================================

-- Use CTEs to get variant_occurrence rows with both per-SNP and per-trait metadata
WITH vo AS (
    SELECT
        vo.variant_occurrence_id,
        vo.rs_id,
        sr.eaf
    FROM cdm.variant_occurrence vo
    JOIN stg.snp_reference sr ON sr.snp_id = vo.rs_id
),
trait_rows AS (
    SELECT
        vo.variant_occurrence_id,
        stm.beta_ZX,
        stm.pval_ZX,
        stm.gwas_source,
        stm.trait_label
    FROM vo
    JOIN stg.snp_trait_map stm ON stm.snp_id = vo.rs_id
)

INSERT INTO cdm.variant_annotation (
    variant_annotation_id,
    variant_occurrence_id,
    annotation_field,
    value_as_string,
    value_as_number
)
-- 1. GWAS beta (effect size)
SELECT
    ROW_NUMBER() OVER (ORDER BY variant_occurrence_id, annotation_field) AS variant_annotation_id,
    variant_occurrence_id,
    annotation_field,
    value_as_string,
    value_as_number
FROM (
    SELECT variant_occurrence_id, 'gwas_beta' AS annotation_field,
           NULL AS value_as_string, beta_ZX AS value_as_number
    FROM trait_rows
    WHERE beta_ZX IS NOT NULL AND beta_ZX != 0

    UNION ALL

    -- 2. GWAS p-value
    SELECT variant_occurrence_id, 'gwas_pvalue',
           NULL, pval_ZX
    FROM trait_rows
    WHERE pval_ZX IS NOT NULL

    UNION ALL

    -- 3. GWAS source (OpenGWAS ID)
    SELECT variant_occurrence_id, 'gwas_source',
           gwas_source, NULL
    FROM trait_rows
    WHERE gwas_source IS NOT NULL

    UNION ALL

    -- 4. Trait association label
    SELECT variant_occurrence_id, 'trait_association',
           trait_label, NULL
    FROM trait_rows
    WHERE trait_label IS NOT NULL

    UNION ALL

    -- 5. Effect allele frequency
    SELECT variant_occurrence_id, 'effect_allele_frequency',
           NULL, eaf
    FROM vo
    WHERE eaf IS NOT NULL
) annotations
ORDER BY variant_occurrence_id, annotation_field;
