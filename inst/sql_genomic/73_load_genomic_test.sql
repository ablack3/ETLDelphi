-- ============================================================================
-- GENOMIC_TEST: single row describing the simulated genotyping panel.
-- ============================================================================

INSERT INTO cdm.genomic_test (
    genomic_test_id,
    care_site_id,
    genomic_test_name,
    genomic_test_version,
    reference_genome,
    sequencing_device,
    library_preparation,
    target_capture,
    read_type,
    read_length,
    quality_control_tools,
    total_reads,
    mean_target_coverage,
    per_target_base_cover_100x,
    alignment_tools,
    variant_calling_tools,
    chromosome_coordinate,
    annotation_tools,
    annotation_databases
)
VALUES (
    1,                                       -- genomic_test_id
    1,                                       -- care_site_id (from existing care_site)
    'Delphi Simulated GWAS Panel',           -- genomic_test_name
    '1.0',                                   -- genomic_test_version
    'GRCh37',                                -- reference_genome (standard for GWAS catalogs)
    NULL,                                    -- sequencing_device (simulated, N/A)
    NULL,                                    -- library_preparation
    NULL,                                    -- target_capture
    NULL,                                    -- read_type
    NULL,                                    -- read_length
    NULL,                                    -- quality_control_tools
    NULL,                                    -- total_reads
    NULL,                                    -- mean_target_coverage
    NULL,                                    -- per_target_base_cover_100x
    NULL,                                    -- alignment_tools
    'ETLDelphi::simulateGenomicData',        -- variant_calling_tools
    NULL,                                    -- chromosome_coordinate
    NULL,                                    -- annotation_tools
    'IEU OpenGWAS'                           -- annotation_databases
);
