-- ============================================================================
-- TARGET_GENE: one row per unique gene from the SNP reference panel.
-- Links each gene (by HGNC ID/symbol) to the genomic_test.
-- ============================================================================

INSERT INTO cdm.target_gene (
    target_gene_id,
    genomic_test_id,
    hgnc_id,
    hgnc_symbol
)
SELECT
    ROW_NUMBER() OVER (ORDER BY hgnc_id) AS target_gene_id,
    1                                    AS genomic_test_id,
    hgnc_id,
    gene_symbol                          AS hgnc_symbol
FROM (
    SELECT DISTINCT gene_symbol, hgnc_id
    FROM stg.snp_reference
    WHERE gene_symbol IS NOT NULL
      AND hgnc_id IS NOT NULL
) genes
ORDER BY hgnc_id;
