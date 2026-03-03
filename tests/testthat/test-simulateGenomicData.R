make_fake_genomic_cdm <- function(person, condition_occurrence = NULL, measurement = NULL) {
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")

  if (is.null(condition_occurrence)) {
    condition_occurrence <- data.frame(
      person_id = integer(0),
      condition_concept_id = integer(0)
    )
  }
  if (is.null(measurement)) {
    measurement <- data.frame(
      person_id = integer(0),
      measurement_concept_id = integer(0),
      measurement_date = as.Date(character(0)),
      value_as_number = numeric(0)
    )
  }

  DBI::dbWriteTable(con, DBI::Id(schema = "main", table = "person"), person, overwrite = TRUE)
  DBI::dbWriteTable(
    con,
    DBI::Id(schema = "main", table = "condition_occurrence"),
    condition_occurrence,
    overwrite = TRUE
  )
  DBI::dbWriteTable(
    con,
    DBI::Id(schema = "main", table = "measurement"),
    measurement,
    overwrite = TRUE
  )

  cdm <- structure(
    list(
      person = dplyr::tbl(con, DBI::Id(schema = "main", table = "person")),
      condition_occurrence = dplyr::tbl(con, DBI::Id(schema = "main", table = "condition_occurrence")),
      measurement = dplyr::tbl(con, DBI::Id(schema = "main", table = "measurement"))
    ),
    class = c("cdm_reference", "list")
  )

  attr(cdm, "cdm_source") <- structure(list(), dbcon = con)
  attr(cdm, "cdm_name") <- "Test CDM"
  attr(cdm, "cdm_version") <- "5.4"
  attr(cdm, "cdm_schema") <- "main"
  attr(cdm, "write_schema") <- c(schema = "main")

  cdm
}

test_that("simulateGenomicData writes staging tables and getGenotypes reads them", {
  skip_if_not_installed("duckdb")

  person <- data.frame(
    person_id = 1:3,
    gender_concept_id = c(0L, 0L, 0L),
    year_of_birth = c(1980L, 1985L, 1990L),
    race_concept_id = c(0L, 0L, 0L)
  )
  condition_occurrence <- data.frame(
    person_id = 1L,
    condition_concept_id = 201826L
  )
  cdm <- make_fake_genomic_cdm(person, condition_occurrence = condition_occurrence)
  con <- CDMConnector::cdmCon(cdm)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  snp_panel <- list(
    list(
      trait_key = "t2d",
      trait_label = "Type 2 diabetes",
      gwas_source = "test",
      proxy_type = "condition",
      proxy_concept_ids = c(201826L),
      proxy_direction = 1L,
      snps = list(
        list(
          snp_id = "rs_test",
          chr = 1L,
          pos = 1001L,
          ea = "A",
          oa = "G",
          eaf = 0.30,
          beta_ZX = 0.25,
          se_ZX = 0.01,
          pval_ZX = 1e-6
        )
      )
    )
  )

  cdm <- simulateGenomicData(
    cdm,
    snpPanel = snp_panel,
    nullSnpCount = 0L,
    verbose = FALSE,
    overwrite = TRUE
  )

  expect_true(DBI::dbExistsTable(con, DBI::Id(schema = "stg", table = "genomic_variants")))
  expect_true(DBI::dbExistsTable(con, DBI::Id(schema = "stg", table = "snp_reference")))
  expect_true(DBI::dbExistsTable(con, DBI::Id(schema = "stg", table = "ancestry_pcs")))

  geno <- getGenotypes(cdm, "rs_test")
  expect_equal(sort(geno$person_id), 1:3)
  expect_true("rs_test" %in% names(geno))
})

test_that("condition phenotype proxies include explicit controls", {
  skip_if_not_installed("duckdb")

  person <- data.frame(
    person_id = 1:3,
    gender_concept_id = c(0L, 0L, 0L),
    year_of_birth = c(1980L, 1985L, 1990L),
    race_concept_id = c(0L, 0L, 0L)
  )
  condition_occurrence <- data.frame(
    person_id = 1L,
    condition_concept_id = 201826L
  )
  cdm <- make_fake_genomic_cdm(person, condition_occurrence = condition_occurrence)
  con <- CDMConnector::cdmCon(cdm)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  snp_ref <- ETLDelphi:::buildSnpReference(
    snpPanel = list(
      list(
        trait_key = "t2d",
        trait_label = "Type 2 diabetes",
        gwas_source = "test",
        proxy_type = "condition",
        proxy_concept_ids = c(201826L),
        proxy_direction = 1L,
        snps = list(
          list(
            snp_id = "rs_test",
            chr = 1L,
            pos = 1001L,
            ea = "A",
            oa = "G",
            eaf = 0.30,
            beta_ZX = 0.25,
            se_ZX = 0.01,
            pval_ZX = 1e-6
          )
        )
      )
    ),
    nullSnpCount = 0L,
    seed = 42L
  )

  pheno <- ETLDelphi:::extractPhenotypeProxies(
    cdm = cdm,
    snp_ref = snp_ref,
    person_ids = person$person_id,
    verbose = FALSE
  )
  pheno <- pheno[order(pheno$person_id), ]

  expect_equal(pheno$person_id, 1:3)
  expect_equal(pheno$proxy_value, c(1, 0, 0))
})

test_that("simulateChunkGenotypes uses ancestry PCs to shift genotype frequencies", {
  person_ids <- 1:4000
  snp_ref <- data.frame(
    snp_id = "rs_pc",
    chromosome = 1L,
    position = 1001L,
    effect_allele = "A",
    other_allele = "G",
    eaf = 0.30,
    beta_ZX = 0,
    se_ZX = 0.01,
    pval_ZX = 1,
    gene_symbol = NA_character_,
    hgnc_id = NA_character_,
    trait_key = "pc_trait",
    trait_label = "PC trait",
    gwas_source = NA_character_,
    proxy_type = "measurement",
    proxy_concept_ids = NA_character_,
    proxy_direction = 1L,
    is_null_snp = FALSE,
    gwas_retrieved = "2026-03-03",
    stringsAsFactors = FALSE
  )
  ld_blocks <- list(list(snps = snp_ref, cor_mat = matrix(1, 1, 1)))
  pheno_data <- data.frame(
    person_id = integer(0),
    trait_key = character(0),
    proxy_value = numeric(0)
  )
  pc_data <- data.frame(
    person_id = person_ids,
    pc1 = c(rep(-3, 2000), rep(3, 2000)),
    pc2 = 0,
    pc3 = 0
  )

  out <- ETLDelphi:::simulateChunkGenotypes(
    person_ids = person_ids,
    snp_ref = snp_ref,
    ld_blocks = ld_blocks,
    pheno_data = pheno_data,
    pc_data = pc_data,
    seed = 42L
  )

  low_pc_mean <- mean(out$genotype[1:2000])
  high_pc_mean <- mean(out$genotype[2001:4000])

  expect_gt(high_pc_mean, low_pc_mean + 0.25)
})
