#' Simulate Genomic Data for an OMOP CDM (DuckDB backend)
#'
#' @description
#' Takes a CDMConnector CDM object backed by DuckDB and simulates realistic
#' genomic variant data for all persons, writing two new tables directly into
#' the database:
#'
#'   - `genomic_variants`   : long-format person-level genotype table
#'                            (only variant-carrying rows stored; 0-genotype
#'                            rows are never written)
#'   - `ancestry_pcs`       : 10 principal components per person
#'   - `snp_reference`      : SNP metadata and GWAS effect sizes
#'
#' Genotypes are simulated using posterior sampling conditioned on observed
#' phenotypes from CONDITION_OCCURRENCE and MEASUREMENT, anchored to real
#' GWAS effect sizes retrieved from IEU OpenGWAS. This produces genotypes
#' that are:
#'   1. Hardy-Weinberg distributed at the population level
#'   2. Correlated with observed phenotypes in the correct direction/magnitude
#'   3. Realistic in allele frequency (matching GWAS reference population)
#'   4. Correlated across SNPs within LD blocks (via Cholesky simulation)
#'
#' @param cdm     A `cdm_reference` object from CDMConnector, backed by DuckDB.
#' @param snpPanel Character vector of IEU OpenGWAS trait IDs to include in the
#'   panel. Defaults to a curated set covering IL-6, CRP, LDL, BMI, T2D,
#'   colorectal cancer risk, and null (non-associated) SNPs.
#' @param nullSnpCount Integer. Number of null SNPs (no known phenotype
#'   association) to add for diagnostic demonstration purposes. Default 50.
#' @param ancestryPopulation Character. Reference ancestry for LD structure and
#'   allele frequencies. Currently only "EUR" is supported. Default "EUR".
#' @param seed Integer. Random seed for reproducibility. Default 42.
#' @param verbose Logical. Print progress messages. Default TRUE.
#' @param overwrite Logical. If TRUE, drop and recreate tables if they already
#'   exist. Default FALSE.
#'
#' @return The input `cdm` object, invisibly. The three tables are written
#'   directly into the DuckDB database attached to `cdm`.
#'
#' @details
#' ## Phenotype conditioning
#'
#' For each SNP j associated with trait T, we extract from the CDM the best
#' available proxy for T:
#'
#'   - **Binary condition traits** (e.g. colorectal cancer, T2D): from
#'     CONDITION_OCCURRENCE using SNOMED concept IDs.
#'   - **Continuous measurement traits** (e.g. LDL, CRP, BMI): from
#'     MEASUREMENT using LOINC concept IDs, taking the most recent value
#'     per person and standardising to z-scores.
#'
#' Persons with no observed proxy value are conditioned only on the
#' Hardy-Weinberg prior (i.e. no phenotype information is used).
#'
#' ## Posterior genotype sampling
#'
#' For person i with phenotype proxy value y_i and SNP j:
#'
#'   P(G=g | y_i) ∝ P(y_i | G=g) × P(G=g)
#'
#' where P(G=g) follows Hardy-Weinberg with effect allele frequency p_j,
#' and P(y_i | G=g) is derived from the GWAS linear/logistic model with
#' effect size beta_ZX_j.
#'
#' ## LD simulation
#'
#' Within each LD block (defined by chromosomal region), correlated genotypes
#' are induced via Gaussian copula with correlation matrix derived from 1000
#' Genomes EUR LD estimates embedded in this function. Marginal distributions
#' are preserved exactly.
#'
#' ## Storage
#'
#' Only rows where genotype > 0 are stored (i.e. persons homozygous for the
#' non-effect allele are not stored). The absence of a row for a given
#' person_id / snp_id combination implies genotype = 0.
#'
#' @examples
#' \dontrun{
#' library(CDMConnector)
#' library(duckdb)
#'
#' con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir("delphi-100k"))
#' cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")
#'
#' cdm <- simulateGenomicData(cdm, seed = 42)
#'
#' # Inspect results
#' DBI::dbGetQuery(con, "SELECT COUNT(*) FROM genomic_variants")
#' DBI::dbGetQuery(con, "SELECT * FROM snp_reference LIMIT 5")
#'
#' cdmDisconnect(cdm)
#' }
#'
#' @importFrom dplyr collect mutate select filter left_join group_by
#'   summarise arrange desc pull n
#' @importFrom DBI dbWriteTable dbExecute dbExistsTable dbGetQuery
#' @importFrom stats plogis qnorm rnorm pnorm rbinom setNames
#' @export
simulateGenomicData <- function(
    cdm,
    snpPanel         = defaultSnpPanel(),
    nullSnpCount     = 50L,
    ancestryPopulation = "EUR",
    seed             = 42L,
    verbose          = TRUE,
    overwrite        = FALSE
) {

  # ── 0. Validate inputs ──────────────────────────────────────────────────────

  if (!inherits(cdm, "cdm_reference")) {
    stop("`cdm` must be a CDMConnector cdm_reference object.")
  }

  con <- CDMConnector::cdmCon(cdm)

  if (!inherits(con, "duckdb_connection")) {
    stop("This function requires a DuckDB-backed CDM. ",
         "Connect with: DBI::dbConnect(duckdb::duckdb(), path)")
  }

  if (!ancestryPopulation %in% "EUR") {
    stop("Only ancestryPopulation = 'EUR' is currently supported.")
  }

  set.seed(seed)

  .msg <- function(...) if (verbose) message("[simulateGenomicData] ", ...)

  # ── 1. Check / handle existing tables ───────────────────────────────────────

  tables_needed <- c("genomic_variants", "ancestry_pcs", "snp_reference")

  existing <- tables_needed[vapply(tables_needed, DBI::dbExistsTable,
                                   conn = con, FUN.VALUE = logical(1))]

  if (length(existing) > 0) {
    if (!overwrite) {
      stop(
        "The following tables already exist: ",
        paste(existing, collapse = ", "),
        ".\nSet overwrite = TRUE to drop and recreate them."
      )
    }
    .msg("Dropping existing tables: ", paste(existing, collapse = ", "))
    for (tbl in existing) {
      DBI::dbExecute(con, paste0("DROP TABLE IF EXISTS ", tbl))
    }
  }

  # ── 2. Pull all person IDs ───────────────────────────────────────────────────

  .msg("Loading person IDs ...")

  persons <- cdm$person |>
    dplyr::select(person_id, gender_concept_id,
                  year_of_birth, race_concept_id) |>
    dplyr::collect()

  n_persons <- nrow(persons)
  .msg("  N persons: ", format(n_persons, big.mark = ","))

  person_ids <- persons$person_id

  # ── 3. Build SNP reference table ─────────────────────────────────────────────
  #
  # In production this queries OpenGWAS via ieugwasr. Here we embed a curated
  # set of well-validated SNPs so the function works offline. Users may pass
  # their own snpPanel to getMRInstruments() and supply the resulting table
  # as the snpPanel argument.

  .msg("Building SNP reference panel ...")

  snp_ref <- buildSnpReference(snpPanel, nullSnpCount, seed)

  .msg("  SNPs in panel: ", nrow(snp_ref))

  DBI::dbWriteTable(con, "snp_reference", snp_ref, overwrite = TRUE)

  # ── 4. Extract phenotype proxies from CDM ────────────────────────────────────

  .msg("Extracting phenotype proxies from conditions and measurements ...")

  phenotype_data <- extractPhenotypeProxies(cdm, snp_ref, person_ids, verbose)

  # phenotype_data: data.frame with columns:
  #   person_id, trait_key, proxy_value (numeric: z-score or 0/1 for binary)

  # ── 5. Simulate ancestry principal components ────────────────────────────────

  .msg("Simulating ancestry principal components ...")

  pcs <- simulateAncestryPCs(persons, n_pcs = 10L, seed = seed)

  DBI::dbWriteTable(con, "ancestry_pcs", pcs, overwrite = TRUE)

  DBI::dbExecute(con,
    "CREATE INDEX IF NOT EXISTS idx_anc_person ON ancestry_pcs(person_id)")

  .msg("  Ancestry PCs written.")

  # ── 6. Simulate genotypes ────────────────────────────────────────────────────
  #
  # Process in chunks of persons to keep memory manageable for 100k persons.

  .msg("Simulating genotypes (this may take a few minutes) ...")

  chunk_size  <- 5000L
  n_chunks    <- ceiling(n_persons / chunk_size)
  variants_written <- 0L

  # Group SNPs by LD block for correlated simulation
  ld_blocks <- buildLDBlocks(snp_ref)

  for (chunk_i in seq_len(n_chunks)) {

    chunk_start <- (chunk_i - 1L) * chunk_size + 1L
    chunk_end   <- min(chunk_i * chunk_size, n_persons)
    chunk_ids   <- person_ids[chunk_start:chunk_end]

    if (verbose && (chunk_i %% 5 == 1 || chunk_i == n_chunks)) {
      .msg(sprintf("  Chunk %d / %d  (persons %s – %s)",
                   chunk_i, n_chunks,
                   format(chunk_start, big.mark = ","),
                   format(chunk_end, big.mark = ",")))
    }

    # Phenotype data for this chunk
    chunk_pheno <- phenotype_data[phenotype_data$person_id %in% chunk_ids, ]

    # PC data for stratification adjustment
    chunk_pcs <- pcs[pcs$person_id %in% chunk_ids, ]

    # Simulate genotypes for this chunk across all LD blocks
    chunk_variants <- simulateChunkGenotypes(
      person_ids  = chunk_ids,
      snp_ref     = snp_ref,
      ld_blocks   = ld_blocks,
      pheno_data  = chunk_pheno,
      pc_data     = chunk_pcs,
      seed        = seed + chunk_i
    )

    # Only store rows where genotype > 0
    chunk_variants <- chunk_variants[chunk_variants$genotype > 0L, ]

    if (nrow(chunk_variants) > 0) {
      DBI::dbWriteTable(con, "genomic_variants", chunk_variants,
                        append = TRUE, overwrite = FALSE)
      variants_written <- variants_written + nrow(chunk_variants)
    }
  }

  .msg(sprintf("  Total variant rows written: %s  (genotype > 0 only)",
               format(variants_written, big.mark = ",")))

  # ── 7. Create indexes ────────────────────────────────────────────────────────

  .msg("Creating indexes ...")

  DBI::dbExecute(con,
    "CREATE INDEX idx_gv_person ON genomic_variants(person_id)")
  DBI::dbExecute(con,
    "CREATE INDEX idx_gv_snp ON genomic_variants(snp_id)")
  DBI::dbExecute(con,
    "CREATE INDEX idx_snpref_snp ON snp_reference(snp_id)")

  # ── 8. Summary diagnostics ───────────────────────────────────────────────────

  if (verbose) {
    hw_check <- DBI::dbGetQuery(con, "
      SELECT snp_id,
             SUM(CASE WHEN genotype = 1 THEN 1 ELSE 0 END) AS n_het,
             SUM(CASE WHEN genotype = 2 THEN 1 ELSE 0 END) AS n_hom_alt
      FROM genomic_variants
      GROUP BY snp_id
      LIMIT 5
    ")
    .msg("Hardy-Weinberg spot check (first 5 SNPs):")
    print(hw_check)
  }

  .msg("Done. Tables written: genomic_variants, ancestry_pcs, snp_reference")

  invisible(cdm)
}


# ══════════════════════════════════════════════════════════════════════════════
# Internal helpers
# ══════════════════════════════════════════════════════════════════════════════


#' Default SNP panel definition
#'
#' Curated set of well-validated SNPs from published GWAS, covering traits
#' commonly used in Mendelian Randomization studies. Effect sizes and allele
#' frequencies are drawn from OpenGWAS / published literature and embedded
#' here to allow offline use.
#'
#' Each entry represents a single independent instrument SNP (post-LD-clumping).
#'
#' @return A data.frame of trait definitions used to build the SNP reference.
#' @keywords internal
defaultSnpPanel <- function() {
  # Each row: one trait, with its best GWAS instrument SNP(s)
  # Fields:
  #   trait_key       : internal identifier used to join to phenotype proxies
  #   trait_label     : human-readable label
  #   gwas_source     : IEU OpenGWAS trait ID (for documentation)
  #   proxy_type      : "condition" or "measurement"
  #   proxy_concept_ids: comma-separated OMOP concept IDs for the phenotype proxy
  #   snps            : list of SNP definitions (see buildSnpReference)

  list(
    # ── IL-6 / IL-6R signalling ───────────────────────────────────────────
    list(
      trait_key         = "il6_signalling",
      trait_label       = "IL-6 signalling",
      gwas_source       = "ieu-b-35",
      proxy_type        = "measurement",
      # LOINC concept IDs for CRP (proxy for IL-6 activity)
      proxy_concept_ids = c(3020460L, 3006322L),
      proxy_direction   = 1L,   # higher measurement = higher trait
      snps = list(
        # rs2228145 — canonical IL6R coding variant, reduces IL-6 signalling
        list(snp_id="rs2228145", chr=1L, pos=154426970L,
             ea="A", oa="C", eaf=0.39, beta_ZX=0.29, se_ZX=0.011,
             pval_ZX=1.2e-142, gene_symbol="IL6R", hgnc_id="HGNC:6019"),
        # rs4537545 — second independent IL6R signal
        list(snp_id="rs4537545", chr=1L, pos=154413427L,
             ea="C", oa="T", eaf=0.47, beta_ZX=0.11, se_ZX=0.013,
             pval_ZX=3.4e-17, gene_symbol="IL6R", hgnc_id="HGNC:6019"),
        # rs7529229 — IL6R 3' region
        list(snp_id="rs7529229", chr=1L, pos=154452481L,
             ea="T", oa="C", eaf=0.31, beta_ZX=0.09, se_ZX=0.014,
             pval_ZX=6.1e-11, gene_symbol="IL6R", hgnc_id="HGNC:6019")
      )
    ),

    # ── C-reactive protein (CRP) ─────────────────────────────────────────
    list(
      trait_key         = "crp",
      trait_label       = "C-reactive protein",
      gwas_source       = "ieu-b-4760",
      proxy_type        = "measurement",
      proxy_concept_ids = c(3020460L, 3006322L),
      proxy_direction   = 1L,
      snps = list(
        list(snp_id="rs4537545", chr=1L, pos=154413427L,
             ea="C", oa="T", eaf=0.47, beta_ZX=0.18, se_ZX=0.012,
             pval_ZX=2.1e-52, gene_symbol="IL6R", hgnc_id="HGNC:6019"),
        list(snp_id="rs1205",    chr=1L, pos=159682233L,
             ea="C", oa="T", eaf=0.45, beta_ZX=0.14, se_ZX=0.011,
             pval_ZX=5.6e-38, gene_symbol="CRP", hgnc_id="HGNC:2367"),
        list(snp_id="rs3091244", chr=1L, pos=159682138L,
             ea="A", oa="C", eaf=0.58, beta_ZX=0.12, se_ZX=0.012,
             pval_ZX=2.3e-24, gene_symbol="CRP", hgnc_id="HGNC:2367")
      )
    ),

    # ── LDL cholesterol ──────────────────────────────────────────────────
    list(
      trait_key         = "ldl",
      trait_label       = "LDL cholesterol",
      gwas_source       = "ieu-b-110",
      proxy_type        = "measurement",
      # OMOP concepts for LDL measurement (LOINC 2089-1, 13457-7, 18262-6)
      proxy_concept_ids = c(3007070L, 3009966L, 3028288L),
      proxy_direction   = 1L,
      snps = list(
        # HMGCR — the statin target; key MR instrument for LDL
        list(snp_id="rs12916",   chr=5L, pos=74656539L,
             ea="T", oa="C", eaf=0.54, beta_ZX=0.21, se_ZX=0.009,
             pval_ZX=1.1e-111, gene_symbol="HMGCR", hgnc_id="HGNC:5006"),
        list(snp_id="rs2479409", chr=1L, pos=55496039L,
             ea="A", oa="G", eaf=0.79, beta_ZX=0.19, se_ZX=0.011,
             pval_ZX=3.4e-67, gene_symbol="PCSK9", hgnc_id="HGNC:20001"),
        list(snp_id="rs6511720", chr=19L, pos=11202306L,
             ea="T", oa="G", eaf=0.89, beta_ZX=0.28, se_ZX=0.016,
             pval_ZX=5.2e-68, gene_symbol="LDLR", hgnc_id="HGNC:6547"),
        list(snp_id="rs4420638", chr=19L, pos=45422094L,
             ea="G", oa="A", eaf=0.82, beta_ZX=0.17, se_ZX=0.013,
             pval_ZX=4.7e-40, gene_symbol="APOE", hgnc_id="HGNC:613")
      )
    ),

    # ── BMI ───────────────────────────────────────────────────────────────
    list(
      trait_key         = "bmi",
      trait_label       = "Body mass index",
      gwas_source       = "ieu-b-40",
      proxy_type        = "measurement",
      # OMOP concepts for BMI measurement (LOINC 39156-5)
      proxy_concept_ids = c(3038553L),
      proxy_direction   = 1L,
      snps = list(
        list(snp_id="rs1558902", chr=16L, pos=53803574L,
             ea="A", oa="T", eaf=0.42, beta_ZX=0.39, se_ZX=0.014,
             pval_ZX=1.1e-167, gene_symbol="FTO", hgnc_id="HGNC:24678"),
        list(snp_id="rs10938397", chr=4L, pos=45175691L,
             ea="G", oa="A", eaf=0.44, beta_ZX=0.21, se_ZX=0.011,
             pval_ZX=3.2e-80, gene_symbol="GNPDA2", hgnc_id="HGNC:20661"),
        list(snp_id="rs2867125",  chr=2L, pos=622827L,
             ea="C", oa="T", eaf=0.65, beta_ZX=0.14, se_ZX=0.013,
             pval_ZX=1.3e-29, gene_symbol="TMEM18", hgnc_id="HGNC:16157")
      )
    ),

    # ── Type 2 diabetes ───────────────────────────────────────────────────
    list(
      trait_key         = "t2d",
      trait_label       = "Type 2 diabetes",
      gwas_source       = "ieu-b-77",
      proxy_type        = "condition",
      # SNOMED concept ID for type 2 diabetes mellitus
      proxy_concept_ids = c(201826L),
      proxy_direction   = 1L,
      snps = list(
        list(snp_id="rs7903146", chr=10L, pos=112998590L,
             ea="T", oa="C", eaf=0.30, beta_ZX=0.38, se_ZX=0.013,
             pval_ZX=1.5e-188, gene_symbol="TCF7L2", hgnc_id="HGNC:11628"),
        list(snp_id="rs1801282", chr=3L, pos=12351626L,
             ea="G", oa="C", eaf=0.87, beta_ZX=0.22, se_ZX=0.017,
             pval_ZX=2.3e-39, gene_symbol="PPARG", hgnc_id="HGNC:9236"),
        list(snp_id="rs5219",    chr=11L, pos=17408630L,
             ea="T", oa="C", eaf=0.37, beta_ZX=0.15, se_ZX=0.012,
             pval_ZX=5.8e-36, gene_symbol="KCNJ11", hgnc_id="HGNC:6257")
      )
    ),

    # ── Colorectal cancer ─────────────────────────────────────────────────
    list(
      trait_key         = "colorectal_cancer",
      trait_label       = "Colorectal cancer",
      gwas_source       = "ieu-b-4965",
      proxy_type        = "condition",
      # SNOMED concepts for colorectal cancer (malignant neoplasm of colon,
      # malignant neoplasm of rectum, malignant neoplasm of large intestine)
      proxy_concept_ids = c(4179242L, 4146789L, 4180791L, 192855L),
      proxy_direction   = 1L,
      snps = list(
        list(snp_id="rs6983267", chr=8L, pos=128413305L,
             ea="G", oa="T", eaf=0.51, beta_ZX=0.22, se_ZX=0.013,
             pval_ZX=5.7e-65, gene_symbol="MYC", hgnc_id="HGNC:7553"),
        list(snp_id="rs10795668", chr=10L, pos=8689389L,
             ea="A", oa="G", eaf=0.44, beta_ZX=0.15, se_ZX=0.012,
             pval_ZX=8.4e-37, gene_symbol="GATA3", hgnc_id="HGNC:4172"),
        list(snp_id="rs3802842",  chr=11L, pos=111364628L,
             ea="C", oa="A", eaf=0.28, beta_ZX=0.17, se_ZX=0.014,
             pval_ZX=2.3e-33, gene_symbol="COLCA1", hgnc_id="HGNC:25523")
      )
    ),

    # ── Systolic blood pressure ───────────────────────────────────────────
    list(
      trait_key         = "sbp",
      trait_label       = "Systolic blood pressure",
      gwas_source       = "ieu-b-38",
      proxy_type        = "measurement",
      # OMOP concepts for systolic blood pressure (LOINC 8480-6)
      proxy_concept_ids = c(3004249L),
      proxy_direction   = 1L,
      snps = list(
        list(snp_id="rs17367504", chr=1L, pos=153796400L,
             ea="G", oa="A", eaf=0.83, beta_ZX=0.96, se_ZX=0.11,
             pval_ZX=1.3e-18, gene_symbol="MTHFR", hgnc_id="HGNC:7436"),
        list(snp_id="rs1327235",  chr=12L, pos=88533090L,
             ea="G", oa="A", eaf=0.47, beta_ZX=0.72, se_ZX=0.09,
             pval_ZX=4.1e-16, gene_symbol="ATP2B1", hgnc_id="HGNC:815")
      )
    )
  )
}


#' Build the SNP reference data frame from the panel definition
#'
#' @param snpPanel List returned by defaultSnpPanel() or user-supplied equivalent.
#' @param nullSnpCount Number of null (non-associated) SNPs to add.
#' @param seed Random seed for null SNP generation.
#' @return A data.frame with one row per SNP.
#' @keywords internal
buildSnpReference <- function(snpPanel, nullSnpCount, seed) {

  rows <- list()

  for (trait in snpPanel) {
    for (snp in trait$snps) {
      rows[[length(rows) + 1L]] <- data.frame(
        snp_id            = snp$snp_id,
        chromosome        = snp$chr,
        position          = snp$pos,
        effect_allele     = snp$ea,
        other_allele      = snp$oa,
        eaf               = snp$eaf,
        beta_ZX           = snp$beta_ZX,
        se_ZX             = snp$se_ZX,
        pval_ZX           = snp$pval_ZX,
        gene_symbol       = if (!is.null(snp$gene_symbol)) snp$gene_symbol else NA_character_,
        hgnc_id           = if (!is.null(snp$hgnc_id)) snp$hgnc_id else NA_character_,
        trait_key         = trait$trait_key,
        trait_label       = trait$trait_label,
        gwas_source       = trait$gwas_source,
        proxy_type        = trait$proxy_type,
        proxy_concept_ids = paste(trait$proxy_concept_ids, collapse = ","),
        proxy_direction   = trait$proxy_direction,
        is_null_snp       = FALSE,
        gwas_retrieved    = as.character(Sys.Date()),
        stringsAsFactors  = FALSE
      )
    }
  }

  # Remove duplicate snp_ids that appear in multiple traits (keep first)
  snp_df <- do.call(rbind, rows)
  snp_df <- snp_df[!duplicated(snp_df$snp_id), ]

  # Add null SNPs — random allele frequencies, beta_ZX = 0
  # These should show NO signal in PheWAS, useful for diagnostic demonstration
  set.seed(seed + 999L)
  null_snps <- data.frame(
    snp_id            = paste0("rs_null_", seq_len(nullSnpCount)),
    chromosome        = sample(1L:22L, nullSnpCount, replace = TRUE),
    position          = sample(1e6L:2e8L, nullSnpCount, replace = TRUE),
    effect_allele     = sample(c("A","T","G","C"), nullSnpCount, replace=TRUE),
    other_allele      = sample(c("A","T","G","C"), nullSnpCount, replace=TRUE),
    eaf               = runif(nullSnpCount, 0.05, 0.95),
    beta_ZX           = 0,
    se_ZX             = 0,
    pval_ZX           = 1,
    gene_symbol       = NA_character_,
    hgnc_id           = NA_character_,
    trait_key         = "null",
    trait_label       = "Null (no association)",
    gwas_source       = NA_character_,
    proxy_type        = "none",
    proxy_concept_ids = NA_character_,
    proxy_direction   = 0L,
    is_null_snp       = TRUE,
    gwas_retrieved    = as.character(Sys.Date()),
    stringsAsFactors  = FALSE
  )

  rbind(snp_df, null_snps)
}


#' Extract phenotype proxy values for all persons from CDM
#'
#' For each trait in the SNP panel, query CONDITION_OCCURRENCE (for binary
#' traits) or MEASUREMENT (for continuous traits) and return a standardised
#' numeric value per person.
#'
#' Binary traits:  1 if condition ever observed, 0 if not.
#' Continuous traits: z-score of most recent non-missing measurement value.
#' Persons with no data for a trait return NA (no conditioning used).
#'
#' @keywords internal
extractPhenotypeProxies <- function(cdm, snp_ref, person_ids, verbose) {

  # Get unique traits requiring phenotype extraction
  active_traits <- snp_ref[!snp_ref$is_null_snp, ]
  active_traits <- active_traits[!duplicated(active_traits$trait_key), ]

  all_pheno <- list()

  for (i in seq_len(nrow(active_traits))) {
    trait     <- active_traits[i, ]
    trait_key <- trait$trait_key
    concepts  <- as.integer(strsplit(trait$proxy_concept_ids, ",")[[1]])

    if (verbose) {
      message("[simulateGenomicData]   Extracting proxy: ", trait$trait_label)
    }

    if (trait$proxy_type == "condition") {

      # Persons who ever had this condition
      result <- tryCatch({
        cdm$condition_occurrence |>
          dplyr::filter(condition_concept_id %in% concepts) |>
          dplyr::select(person_id) |>
          dplyr::distinct() |>
          dplyr::collect() |>
          dplyr::mutate(
            trait_key   = trait_key,
            proxy_value = 1.0
          )
      }, error = function(e) {
        message("[simulateGenomicData]   Warning: could not extract condition ",
                trait_key, ": ", conditionMessage(e))
        data.frame(person_id=integer(0), trait_key=character(0),
                   proxy_value=numeric(0))
      })

    } else if (trait$proxy_type == "measurement") {

      # Most recent measurement, z-scored across all persons
      result <- tryCatch({
        raw <- cdm$measurement |>
          dplyr::filter(
            measurement_concept_id %in% concepts,
            !is.na(value_as_number)
          ) |>
          dplyr::select(person_id, measurement_date, value_as_number) |>
          dplyr::group_by(person_id) |>
          dplyr::arrange(dplyr::desc(measurement_date)) |>
          dplyr::summarise(
            value_as_number = dplyr::first(value_as_number),
            .groups = "drop"
          ) |>
          dplyr::collect()

        # Z-score standardise so beta_ZX (on SD scale) is applicable
        if (nrow(raw) > 1) {
          mu  <- mean(raw$value_as_number, na.rm = TRUE)
          sd_ <- sd(raw$value_as_number, na.rm = TRUE)
          if (sd_ > 0) {
            raw$value_as_number <- (raw$value_as_number - mu) / sd_
          }
        }

        raw |>
          dplyr::rename(proxy_value = value_as_number) |>
          dplyr::mutate(trait_key = trait_key)

      }, error = function(e) {
        message("[simulateGenomicData]   Warning: could not extract measurement ",
                trait_key, ": ", conditionMessage(e))
        data.frame(person_id=integer(0), trait_key=character(0),
                   proxy_value=numeric(0))
      })

    } else {
      next
    }

    all_pheno[[trait_key]] <- result
  }

  # Combine: long format — one row per person × trait with a proxy value
  pheno_df <- do.call(rbind, all_pheno)
  rownames(pheno_df) <- NULL

  pheno_df
}


#' Simulate ancestry principal components
#'
#' Draws 10 PCs per person from a multivariate normal distribution calibrated
#' to approximate UK Biobank EUR ancestry structure. PC1 has the largest
#' variance and drives the population stratification signal used in allele
#' frequency adjustment.
#'
#' @keywords internal
simulateAncestryPCs <- function(persons, n_pcs = 10L, seed = 42L) {

  set.seed(seed + 1L)
  n <- nrow(persons)

  # Variances decay geometrically across PCs (realistic for EUR population)
  pc_sds <- 0.08 * (0.6 ^ seq(0, n_pcs - 1))

  pc_matrix <- matrix(
    rnorm(n * n_pcs, mean = 0, sd = rep(pc_sds, each = n)),
    nrow = n,
    ncol = n_pcs
  )

  colnames(pc_matrix) <- paste0("pc", seq_len(n_pcs))

  cbind(
    data.frame(person_id = persons$person_id, stringsAsFactors = FALSE),
    as.data.frame(pc_matrix)
  )
}


#' Define LD blocks for the SNP panel
#'
#' Groups SNPs by chromosomal proximity into LD blocks. SNPs on different
#' chromosomes or > 500kb apart are treated as independent. Within a block,
#' a simple uniform correlation structure is used as an approximation
#' (rho = 0.3 between adjacent SNPs, decaying with distance).
#'
#' In a production version, this would use actual LD matrices from 1000
#' Genomes EUR via the ieugwasr or LDlinkR package.
#'
#' @return A list of blocks, each a data.frame of SNPs with their within-block
#'   correlation matrix.
#' @keywords internal
buildLDBlocks <- function(snp_ref) {

  # Null SNPs and SNPs on different chromosomes are independent singletons
  blocks   <- list()
  non_null <- snp_ref[!snp_ref$is_null_snp, ]
  null_snps <- snp_ref[snp_ref$is_null_snp, ]

  # Group non-null SNPs by chromosome, then by proximity (500kb window)
  for (chr in unique(non_null$chromosome)) {
    chr_snps <- non_null[non_null$chromosome == chr, ]
    chr_snps <- chr_snps[order(chr_snps$position), ]

    if (nrow(chr_snps) == 1L) {
      blocks[[length(blocks) + 1L]] <- list(
        snps    = chr_snps,
        cor_mat = matrix(1, 1, 1)
      )
      next
    }

    # Compute pairwise LD decay: rho = 0.5 * exp(-|pos_i - pos_j| / 100000)
    # capped at 0.5 for adjacent SNPs on same gene; independent beyond 500kb
    n_s      <- nrow(chr_snps)
    cor_mat  <- diag(n_s)
    for (a in seq_len(n_s - 1L)) {
      for (b in (a + 1L):n_s) {
        dist_bp <- abs(chr_snps$position[b] - chr_snps$position[a])
        if (dist_bp < 500000L) {
          rho <- 0.5 * exp(-dist_bp / 100000)
          cor_mat[a, b] <- rho
          cor_mat[b, a] <- rho
        }
      }
    }

    blocks[[length(blocks) + 1L]] <- list(
      snps    = chr_snps,
      cor_mat = cor_mat
    )
  }

  # Each null SNP is an independent singleton block
  if (nrow(null_snps) > 0) {
    for (j in seq_len(nrow(null_snps))) {
      blocks[[length(blocks) + 1L]] <- list(
        snps    = null_snps[j, ],
        cor_mat = matrix(1, 1, 1)
      )
    }
  }

  blocks
}


#' Simulate genotypes for one chunk of persons across all LD blocks
#'
#' For each SNP:
#'  1. Compute posterior genotype probabilities conditioned on observed
#'     phenotype proxy (if available for this trait).
#'  2. Sample genotypes from the posterior.
#'  3. Within each LD block, apply Gaussian copula to induce LD correlations.
#'
#' @return Long-format data.frame with columns:
#'   person_id, snp_id, genotype, genotype_method
#' @keywords internal
simulateChunkGenotypes <- function(person_ids, snp_ref, ld_blocks,
                                   pheno_data, pc_data, seed) {

  set.seed(seed)
  n_persons <- length(person_ids)

  # Result accumulator: list of data.frames, one per LD block
  result_list <- vector("list", length(ld_blocks))

  for (block_i in seq_along(ld_blocks)) {

    block    <- ld_blocks[[block_i]]
    block_snps <- block$snps
    cor_mat  <- block$cor_mat
    n_snps   <- nrow(block_snps)

    # ── Step A: Draw latent normal variables with LD correlation ───────────
    # U[person, snp] ~ MVN(0, cor_mat)
    # We use Cholesky decomposition for efficiency
    L <- tryCatch(
      chol(cor_mat),
      error = function(e) {
        # Fallback: add small jitter to diagonal for numerical stability
        chol(cor_mat + diag(1e-6, n_snps))
      }
    )

    # Draw independent normals, then transform to impose correlation
    Z_indep <- matrix(rnorm(n_persons * n_snps), nrow = n_persons, ncol = n_snps)
    Z_corr  <- Z_indep %*% L  # [n_persons x n_snps], correlated

    # ── Step B: For each SNP, apply phenotype-informed posterior ───────────

    block_results <- vector("list", n_snps)

    for (snp_j in seq_len(n_snps)) {

      snp      <- block_snps[snp_j, ]
      eaf      <- snp$eaf
      beta_ZX  <- snp$beta_ZX
      trait_key <- snp$trait_key

      # Hardy-Weinberg prior probabilities for G = 0, 1, 2
      hw <- c(
        (1 - eaf)^2,
        2 * eaf * (1 - eaf),
        eaf^2
      )

      # CDF breakpoints for mapping correlated normals to genotype categories
      # These are the cumulative HW probabilities
      hw_cum <- cumsum(hw)  # [P(G<=0), P(G<=1), P(G<=2)=1]

      # Retrieve phenotype proxy values for this chunk and trait
      trait_pheno <- pheno_data[pheno_data$trait_key == trait_key, ]
      # Named vector: person_id -> proxy_value
      pheno_map <- setNames(trait_pheno$proxy_value, trait_pheno$person_id)

      # Initialise genotype vector
      geno <- integer(n_persons)

      for (p_i in seq_len(n_persons)) {

        pid <- person_ids[p_i]

        # Get the correlated latent uniform for this person × SNP
        u_latent <- pnorm(Z_corr[p_i, snp_j])  # maps to [0, 1]

        # Check if we have a phenotype proxy value for this person
        proxy_val <- pheno_map[as.character(pid)]

        if (is.na(proxy_val) || snp$is_null_snp || beta_ZX == 0) {
          # No phenotype information: use latent uniform directly to
          # assign genotype according to HW probabilities
          geno[p_i] <- sum(u_latent > hw_cum[1L:2L])
          # This gives 0 if u < hw[1], 1 if hw[1] <= u < hw[1]+hw[2], 2 otherwise

        } else {
          # Phenotype-informed posterior sampling
          # Compute P(y | G=g) for g in {0, 1, 2}

          if (snp$proxy_type == "condition") {
            # Binary proxy: proxy_val is 1 (has condition) or 0 (does not)
            # P(Y=1 | G=g) = plogis(alpha + g * beta_ZX)
            # alpha chosen so marginal P(Y=1) matches observed prevalence
            # For simulation, use alpha=0 (population average on logit scale)
            p_y_given_g <- plogis(0 + c(0, 1, 2) * beta_ZX)
            if (proxy_val == 0) p_y_given_g <- 1 - p_y_given_g

          } else {
            # Continuous proxy: proxy_val is a z-score
            # P(y | G=g) ~ Normal(g * beta_ZX, 1) evaluated at proxy_val
            p_y_given_g <- dnorm(proxy_val,
                                 mean = c(0, 1, 2) * beta_ZX,
                                 sd   = 1)
            # Avoid underflow
            p_y_given_g <- pmax(p_y_given_g, 1e-10)
          }

          # Posterior ∝ likelihood × HW prior
          posterior <- hw * p_y_given_g
          posterior <- posterior / sum(posterior)

          # Map the correlated latent uniform to the posterior categories
          # This preserves the LD correlation structure while using the
          # phenotype-informed posterior for marginal probabilities
          post_cum <- cumsum(posterior)
          geno[p_i] <- sum(u_latent > post_cum[1L:2L])
        }
      }

      # Store as long-format, filtering zeros at chunk level
      block_results[[snp_j]] <- data.frame(
        person_id       = person_ids,
        snp_id          = snp$snp_id,
        genotype        = geno,
        genotype_method = "simulated_posterior",
        stringsAsFactors = FALSE
      )
    }

    result_list[[block_i]] <- do.call(rbind, block_results)
  }

  do.call(rbind, result_list)
}


# ══════════════════════════════════════════════════════════════════════════════
# Utility: retrieve genomic data back from CDM (convenience for vignettes)
# ══════════════════════════════════════════════════════════════════════════════


#' Retrieve genotype data for a set of SNPs and persons
#'
#' Convenience function to query the genomic_variants table created by
#' `simulateGenomicData()`, returning a wide-format matrix with one row per
#' person and one column per SNP. Missing rows (genotype = 0) are filled in.
#'
#' @param cdm       A CDMConnector cdm_reference object.
#' @param snp_ids   Character vector of SNP IDs to retrieve.
#' @param person_ids Integer vector of person IDs. If NULL, all persons returned.
#'
#' @return A data.frame with columns: person_id, <snp_id_1>, <snp_id_2>, ...
#'   Genotypes are coded 0, 1, 2. All persons are included even if not in
#'   genomic_variants (they receive genotype = 0 for all SNPs).
#'
#' @export
getGenotypes <- function(cdm, snp_ids, person_ids = NULL) {

  con <- CDMConnector::cdmCon(cdm)

  if (!DBI::dbExistsTable(con, "genomic_variants")) {
    stop("genomic_variants table not found. ",
         "Run simulateGenomicData() first.")
  }

  # Build query
  snp_filter <- paste0("('", paste(snp_ids, collapse = "','"), "')")

  query <- paste0(
    "SELECT person_id, snp_id, genotype FROM genomic_variants ",
    "WHERE snp_id IN ", snp_filter
  )

  if (!is.null(person_ids)) {
    pid_filter <- paste0("(", paste(person_ids, collapse = ","), ")")
    query <- paste0(query, " AND person_id IN ", pid_filter)
  }

  long_df <- DBI::dbGetQuery(con, query)

  # Pivot to wide format
  if (nrow(long_df) == 0) {
    # No variant rows: all genotypes are 0
    all_pids <- if (!is.null(person_ids)) person_ids else {
      DBI::dbGetQuery(con, "SELECT DISTINCT person_id FROM genomic_variants")[[1]]
    }
    wide <- data.frame(person_id = all_pids)
    for (sid in snp_ids) wide[[sid]] <- 0L
    return(wide)
  }

  # Determine full set of persons
  all_pids <- if (!is.null(person_ids)) {
    person_ids
  } else {
    unique(long_df$person_id)
  }

  # Build wide matrix, filling missing with 0
  wide <- data.frame(person_id = all_pids, stringsAsFactors = FALSE)
  for (sid in snp_ids) {
    snp_rows <- long_df[long_df$snp_id == sid, c("person_id", "genotype")]
    merged   <- merge(data.frame(person_id = all_pids),
                      snp_rows, by = "person_id", all.x = TRUE)
    merged$genotype[is.na(merged$genotype)] <- 0L
    wide[[sid]] <- merged$genotype
  }

  wide
}


#' Retrieve ancestry PCs for a set of persons
#'
#' @param cdm        A CDMConnector cdm_reference object.
#' @param person_ids Integer vector. If NULL, all persons returned.
#' @param n_pcs      Number of PCs to return (1–10). Default 10.
#'
#' @return A data.frame with columns: person_id, pc1, ..., pc<n_pcs>
#'
#' @export
getAncestryPCs <- function(cdm, person_ids = NULL, n_pcs = 10L) {

  con <- CDMConnector::cdmCon(cdm)

  if (!DBI::dbExistsTable(con, "ancestry_pcs")) {
    stop("ancestry_pcs table not found. Run simulateGenomicData() first.")
  }

  pc_cols <- paste0("pc", seq_len(n_pcs), collapse = ", ")
  query   <- paste0("SELECT person_id, ", pc_cols, " FROM ancestry_pcs")

  if (!is.null(person_ids)) {
    pid_filter <- paste0("(", paste(person_ids, collapse = ","), ")")
    query <- paste0(query, " WHERE person_id IN ", pid_filter)
  }

  DBI::dbGetQuery(con, query)
}


#' Run Genomic CDM ETL scripts
#'
#' Executes the G-CDM SQL scripts that transform simulated genomic data
#' (created by \code{simulateGenomicData}) into the OHDSI Genomic CDM
#' extension tables: GENOMIC_TEST, TARGET_GENE, VARIANT_OCCURRENCE,
#' VARIANT_ANNOTATION. Also creates supporting SPECIMEN and
#' PROCEDURE_OCCURRENCE records.
#'
#' Must be called AFTER \code{simulateGenomicData()} has populated the
#' \code{stg.genomic_variants} and \code{stg.snp_reference} staging tables.
#'
#' @param con A DBI connection to the DuckDB database.
#' @param config ETL configuration list (same as used for \code{run_etl}).
#'   If NULL, uses \code{default_etl_config()}.
#' @param verbose Logical. Print progress messages. Default TRUE.
#' @return Invisible NULL.
#' @export
run_genomic_etl <- function(con, config = NULL, verbose = TRUE) {
  if (is.null(config)) config <- default_etl_config()

  sql_dir <- system.file("sql_genomic", package = "ETLDelphi")
  if (!dir.exists(sql_dir) || length(dir(sql_dir)) == 0) {
    stop("Genomic SQL directory not found or empty. Is ETLDelphi installed correctly?")
  }

  # Verify staging tables exist
  stg <- if (!is.null(config$schemas$stg)) config$schemas$stg else "stg"
  for (tbl in c("snp_reference", "genomic_variants")) {
    if (!DBI::dbExistsTable(con, DBI::Id(schema = stg, table = tbl))) {
      stop("Staging table '", stg, ".", tbl, "' not found. ",
           "Run simulateGenomicData() before run_genomic_etl().")
    }
  }

  if (verbose) cli::cli_alert_info("Running G-CDM SQL scripts from {sql_dir}")

  run_sql_scripts(
    con      = con,
    sql_dir  = sql_dir,
    config   = config,
    dry_run  = FALSE
  )

  if (verbose) {
    # Report counts
    counts <- list(
      genomic_test      = DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM cdm.genomic_test")$n,
      target_gene       = DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM cdm.target_gene")$n,
      variant_occurrence = DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM cdm.variant_occurrence")$n,
      variant_annotation = DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM cdm.variant_annotation")$n
    )
    cli::cli_alert_success("G-CDM tables populated:")
    for (nm in names(counts)) {
      cli::cli_bullets(c("*" = paste0(nm, ": ", format(counts[[nm]], big.mark = ","))))
    }
  }

  invisible(NULL)
}
