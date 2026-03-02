-- ============================================================================
-- VARIANT_OCCURRENCE: one row per person × SNP where genotype > 0.
-- Maps simulated genomic_variants + snp_reference → G-CDM variant_occurrence.
--
-- genotype encoding:
--   0 = homozygous reference (not stored — no variant)
--   1 = heterozygous
--   2 = homozygous alternate
--
-- allele mapping:
--   reference_allele = other_allele (the non-effect allele)
--   alternate_allele = effect_allele (the GWAS effect allele)
-- ============================================================================

INSERT INTO cdm.variant_occurrence (
    variant_occurrence_id,
    procedure_occurrence_id,
    specimen_id,
    reference_specimen_id,
    target_gene1_id,
    target_gene1_symbol,
    target_gene2_id,
    target_gene2_symbol,
    reference_sequence,
    rs_id,
    reference_allele,
    alternate_allele,
    hgvs_c,
    hgvs_p,
    variant_read_depth,
    variant_exon_number,
    copy_number,
    cnv_locus,
    fusion_breakpoint,
    fusion_supporting_reads,
    sequence_alteration,
    variant_feature,
    genetic_origin,
    genotype
)
SELECT
    ROW_NUMBER() OVER (ORDER BY gv.person_id, sr.snp_id) AS variant_occurrence_id,
    800000000 + gv.person_id     AS procedure_occurrence_id,
    700000000 + gv.person_id     AS specimen_id,
    NULL                         AS reference_specimen_id,
    tg.target_gene_id::VARCHAR   AS target_gene1_id,
    sr.gene_symbol               AS target_gene1_symbol,
    NULL                         AS target_gene2_id,
    NULL                         AS target_gene2_symbol,
    'GRCh37'                     AS reference_sequence,
    sr.snp_id                    AS rs_id,
    sr.other_allele              AS reference_allele,
    sr.effect_allele             AS alternate_allele,
    NULL                         AS hgvs_c,
    NULL                         AS hgvs_p,
    NULL                         AS variant_read_depth,
    NULL                         AS variant_exon_number,
    NULL                         AS copy_number,
    NULL                         AS cnv_locus,
    NULL                         AS fusion_breakpoint,
    NULL                         AS fusion_supporting_reads,
    'SNP'                        AS sequence_alteration,
    NULL                         AS variant_feature,
    'germline'                   AS genetic_origin,
    CASE gv.genotype
        WHEN 1 THEN 'heterozygous'
        WHEN 2 THEN 'homozygous alternate'
    END                          AS genotype
FROM stg.genomic_variants gv
JOIN stg.snp_reference sr ON sr.snp_id = gv.snp_id
LEFT JOIN cdm.target_gene tg
    ON tg.hgnc_symbol = sr.gene_symbol
    AND tg.genomic_test_id = 1
WHERE gv.genotype > 0;
