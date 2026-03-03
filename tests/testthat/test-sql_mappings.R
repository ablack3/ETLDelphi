test_that("default_etl_config uses valid defaults for configurable concepts", {
  cfg <- default_etl_config()
  tc <- cfg$type_concept_ids

  expect_identical(tc$visit_type_concept_id, 44818518L)
  expect_identical(tc$default_visit_concept_id, 9202L)
  expect_identical(tc$condition_type_concept_id, 38000245L)
  expect_identical(tc$drug_type_dispensed, 38000175L)
  expect_identical(tc$observation_type_allergy, 38000280L)
  expect_identical(tc$note_type_concept_id, 32831L)
  expect_identical(tc$note_class_concept_id, 3000735L)
  expect_identical(tc$encoding_concept_id, 32678L)
  expect_identical(tc$procedure_type_concept_id, 38000275L)
  expect_identical(tc$death_type_concept_id, 32510L)
})

test_that("visit concept mapping treats null clinic rules as wildcards", {
  skip_if_not_installed("duckdb")

  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE SCHEMA stg")
  DBI::dbExecute(con, "CREATE TABLE stg.encounter (appt_type VARCHAR, clinic_type VARCHAR)")
  DBI::dbExecute(
    con,
    "INSERT INTO stg.encounter VALUES ('Emergency', 'Hospital'), ('Inpatient', 'Acute'), ('Outpatient', 'Specialty')"
  )

  sql_path <- system.file("sql", "30_vocab", "42_map_visit_concept.sql", package = "ETLDelphi", mustWork = TRUE)
  sql <- paste(readLines(sql_path, warn = FALSE), collapse = "\n")
  sql <- gsub("\\{default_visit_concept_id\\}", "9202", sql)
  sql <- gsub("--[^\n]*", "\n", sql)
  for (stmt in ETLDelphi:::split_sql_statements(sql)) {
    stmt <- trimws(stmt)
    if (!nzchar(stmt)) next
    DBI::dbExecute(con, stmt)
  }

  out <- DBI::dbGetQuery(
    con,
    "SELECT appt_type, clinic_type, visit_concept_id FROM stg.map_visit_concept ORDER BY appt_type"
  )
  expect_equal(out$visit_concept_id[out$appt_type == "emergency"], 9203)
  expect_equal(out$visit_concept_id[out$appt_type == "inpatient"], 9201)
  expect_equal(out$visit_concept_id[out$appt_type == "outpatient"], 9202)
})

test_that("therapy mapping respects the supplied source vocabulary", {
  skip_if_not_installed("duckdb")

  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE SCHEMA stg")
  DBI::dbExecute(con, "CREATE SCHEMA cdm")
  DBI::dbExecute(con, "CREATE TABLE stg.therapy_orders (code VARCHAR, vocabulary VARCHAR)")
  DBI::dbExecute(con, "CREATE TABLE stg.therapy_actions (code VARCHAR, vocabulary VARCHAR)")
  DBI::dbExecute(con, "INSERT INTO stg.therapy_orders VALUES ('77', 'ICD9Proc'), ('77', 'CPT')")
  DBI::dbExecute(
    con,
    "CREATE TABLE cdm.concept (
      concept_id INTEGER,
      concept_code VARCHAR,
      vocabulary_id VARCHAR,
      invalid_reason VARCHAR,
      standard_concept VARCHAR
    )"
  )
  DBI::dbExecute(
    con,
    "CREATE TABLE cdm.concept_relationship (
      concept_id_1 INTEGER,
      concept_id_2 INTEGER,
      relationship_id VARCHAR,
      invalid_reason VARCHAR
    )"
  )
  DBI::dbExecute(
    con,
    "INSERT INTO cdm.concept VALUES
      (100, '77', 'CPT4', NULL, 'S'),
      (200, '77', 'ICD9Proc', NULL, 'S')"
  )
  DBI::dbExecute(
    con,
    "INSERT INTO cdm.concept_relationship VALUES
      (100, 100, 'Maps to', NULL),
      (200, 200, 'Maps to', NULL)"
  )

  sql_path <- system.file("sql", "30_vocab", "49_map_therapy.sql", package = "ETLDelphi", mustWork = TRUE)
  sql <- paste(readLines(sql_path, warn = FALSE), collapse = "\n")
  sql <- gsub("--[^\n]*", "\n", sql)
  for (stmt in ETLDelphi:::split_sql_statements(sql)) {
    stmt <- trimws(stmt)
    if (!nzchar(stmt)) next
    DBI::dbExecute(con, stmt)
  }

  out <- DBI::dbGetQuery(
    con,
    "SELECT code, vocabulary, procedure_concept_id, procedure_source_concept_id FROM stg.map_therapy ORDER BY vocabulary"
  )
  expect_equal(out$procedure_concept_id[out$vocabulary == "CPT"], 100)
  expect_equal(out$procedure_source_concept_id[out$vocabulary == "CPT"], 100)
  expect_equal(out$procedure_concept_id[out$vocabulary == "ICD9Proc"], 200)
  expect_equal(out$procedure_source_concept_id[out$vocabulary == "ICD9Proc"], 200)
})

test_that("core ETL SQL uses configurable placeholders for type concepts", {
  checks <- list(
    c("40_cdm_core", "54_load_visit_occurrence.sql", "{visit_type_concept_id}"),
    c("40_cdm_core", "54_load_visit_occurrence.sql", "{default_visit_concept_id}"),
    c("40_cdm_core", "55_load_note.sql", "{note_type_concept_id}"),
    c("40_cdm_core", "55_load_note.sql", "{note_class_concept_id}"),
    c("40_cdm_core", "55_load_note.sql", "{encoding_concept_id}"),
    c("40_cdm_core", "55_load_note.sql", "{language_concept_id}"),
    c("50_cdm_clinical", "61_load_drug_exposure_orders.sql", "{drug_type_orders}"),
    c("50_cdm_clinical", "62_load_drug_exposure_fulfillment.sql", "{drug_type_dispensed}"),
    c("50_cdm_clinical", "63_load_drug_exposure_immunization.sql", "{drug_type_immunization}"),
    c("50_cdm_clinical", "66_load_observation_allergy.sql", "{observation_type_allergy}"),
    c("50_cdm_clinical", "67_load_procedure_therapy.sql", "{procedure_type_concept_id}")
  )

  for (check in checks) {
    sql_path <- system.file("sql", check[[1]], check[[2]], package = "ETLDelphi", mustWork = TRUE)
    sql <- paste(readLines(sql_path, warn = FALSE), collapse = "\n")
    expect_true(grepl(check[[3]], sql, fixed = TRUE), info = basename(sql_path))
  }
})
