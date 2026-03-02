-- ============================================================================
-- G-CDM v2.0 DDL for DuckDB — Genomic CDM extension tables
-- Converted from OHDSI-SQL (@cdm_schema parameterized) to DuckDB syntax.
-- Reference: https://github.com/OHDSI/Genomic-CDM
-- ============================================================================

-- GENOMIC_TEST
CREATE TABLE IF NOT EXISTS cdm.genomic_test (
    genomic_test_id            INTEGER       NOT NULL,
    care_site_id               INTEGER       NOT NULL,
    genomic_test_name          VARCHAR(255),
    genomic_test_version       VARCHAR(50),
    reference_genome           VARCHAR(50),
    sequencing_device          VARCHAR(50),
    library_preparation        VARCHAR(50),
    target_capture             VARCHAR(50),
    read_type                  VARCHAR(50),
    read_length                INTEGER,
    quality_control_tools      VARCHAR(255),
    total_reads                INTEGER,
    mean_target_coverage       FLOAT,
    per_target_base_cover_100x FLOAT,
    alignment_tools            VARCHAR(255),
    variant_calling_tools      VARCHAR(255),
    chromosome_coordinate      VARCHAR(255),
    annotation_tools           VARCHAR(255),
    annotation_databases       VARCHAR(255),
    PRIMARY KEY (genomic_test_id)
    -- FK: care_site_id -> care_site
);

-- TARGET_GENE
CREATE TABLE IF NOT EXISTS cdm.target_gene (
    target_gene_id  INTEGER      NOT NULL,
    genomic_test_id INTEGER      NOT NULL,
    hgnc_id         VARCHAR(50)  NOT NULL,
    hgnc_symbol     VARCHAR(50)  NOT NULL,
    PRIMARY KEY (target_gene_id)
    -- FK: genomic_test_id -> genomic_test
);

-- VARIANT_OCCURRENCE
CREATE TABLE IF NOT EXISTS cdm.variant_occurrence (
    variant_occurrence_id    INTEGER       NOT NULL,
    procedure_occurrence_id  INTEGER       NOT NULL,
    specimen_id              INTEGER       NOT NULL,
    reference_specimen_id    INTEGER,
    target_gene1_id          VARCHAR(50),
    target_gene1_symbol      VARCHAR(255),
    target_gene2_id          VARCHAR(50),
    target_gene2_symbol      VARCHAR(255),
    reference_sequence       VARCHAR(50),
    rs_id                    VARCHAR(50),
    reference_allele         VARCHAR(255),
    alternate_allele         VARCHAR(255),
    hgvs_c                   VARCHAR,
    hgvs_p                   VARCHAR,
    variant_read_depth       INTEGER,
    variant_exon_number      INTEGER,
    copy_number              FLOAT,
    cnv_locus                VARCHAR,
    fusion_breakpoint        VARCHAR,
    fusion_supporting_reads  INTEGER,
    sequence_alteration      VARCHAR,
    variant_feature          VARCHAR,
    genetic_origin           VARCHAR(50),
    genotype                 VARCHAR(50),
    PRIMARY KEY (variant_occurrence_id)
    -- FK: procedure_occurrence_id -> procedure_occurrence
    -- FK: specimen_id -> specimen
    -- FK: reference_specimen_id -> specimen
);

-- VARIANT_ANNOTATION
CREATE TABLE IF NOT EXISTS cdm.variant_annotation (
    variant_annotation_id INTEGER       NOT NULL,
    variant_occurrence_id INTEGER       NOT NULL,
    annotation_field      VARCHAR       NOT NULL,
    value_as_string       VARCHAR,
    value_as_number       FLOAT,
    PRIMARY KEY (variant_annotation_id)
    -- FK: variant_occurrence_id -> variant_occurrence
);

-- Indexes for key lookups
CREATE INDEX IF NOT EXISTS ix_variant_occurrence_procedure
    ON cdm.variant_occurrence (procedure_occurrence_id);

CREATE INDEX IF NOT EXISTS ix_variant_annotation_variant
    ON cdm.variant_annotation (variant_occurrence_id);
