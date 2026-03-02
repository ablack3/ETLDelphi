-- ============================================================================
-- VARIANT_ANNOTATION: GWAS annotation rows for each variant occurrence.
-- Creates multiple annotation rows per variant:
--   1. gwas_beta         → value_as_number (GWAS effect size)
--   2. gwas_pvalue       → value_as_number (GWAS p-value)
--   3. gwas_source       → value_as_string (IEU OpenGWAS ID)
--   4. trait_association  → value_as_string (trait label)
--   5. effect_allele_frequency → value_as_number (EAF)
-- ============================================================================

-- Use a CTE to get variant_occurrence rows with their SNP metadata
WITH vo AS (
    SELECT
        vo.variant_occurrence_id,
        vo.rs_id,
        sr.beta_ZX,
        sr.pval_ZX,
        sr.gwas_source,
        sr.trait_label,
        sr.eaf
    FROM cdm.variant_occurrence vo
    JOIN stg.snp_reference sr ON sr.snp_id = vo.rs_id
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
    FROM vo
    WHERE beta_ZX IS NOT NULL AND beta_ZX != 0

    UNION ALL

    -- 2. GWAS p-value
    SELECT variant_occurrence_id, 'gwas_pvalue',
           NULL, pval_ZX
    FROM vo
    WHERE pval_ZX IS NOT NULL

    UNION ALL

    -- 3. GWAS source (OpenGWAS ID)
    SELECT variant_occurrence_id, 'gwas_source',
           gwas_source, NULL
    FROM vo
    WHERE gwas_source IS NOT NULL

    UNION ALL

    -- 4. Trait association label
    SELECT variant_occurrence_id, 'trait_association',
           trait_label, NULL
    FROM vo
    WHERE trait_label IS NOT NULL

    UNION ALL

    -- 5. Effect allele frequency
    SELECT variant_occurrence_id, 'effect_allele_frequency',
           NULL, eaf
    FROM vo
    WHERE eaf IS NOT NULL
) annotations
ORDER BY variant_occurrence_id, annotation_field;
