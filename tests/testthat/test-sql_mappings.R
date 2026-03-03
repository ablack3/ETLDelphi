test_that("default_etl_config uses valid defaults for configurable concepts", {
  cfg <- default_etl_config()
  tc <- cfg$type_concept_ids

  expect_identical(tc$visit_type_concept_id, 32827L)
  expect_identical(tc$default_visit_concept_id, 9202L)
  expect_identical(tc$condition_type_concept_id, 32840L)
  expect_identical(tc$drug_type_orders, 32838L)
  expect_identical(tc$drug_type_dispensed, 32825L)
  expect_identical(tc$measurement_type_vitals, 32836L)
  expect_identical(tc$measurement_type_labs, 32856L)
  expect_identical(tc$observation_type_allergy, 32817L)
  expect_identical(tc$note_type_concept_id, 32831L)
  expect_identical(tc$note_class_concept_id, 3000735L)
  expect_identical(tc$encoding_concept_id, 32678L)
  expect_identical(tc$procedure_type_concept_id, 32833L)
  expect_identical(tc$death_type_concept_id, 32817L)
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

test_that("lab measurement loading parses numeric text and limits categorical fallback", {
  skip_if_not_installed("duckdb")

  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE SCHEMA stg")
  DBI::dbExecute(con, "CREATE SCHEMA cdm")
  DBI::dbExecute(
    con,
    "CREATE TABLE stg.lab_results (
      member_id VARCHAR,
      test_loinc VARCHAR,
      test_name VARCHAR,
      date_resulted DATE,
      date_collected DATE,
      date_resulted_datetime TIMESTAMP,
      date_collected_datetime TIMESTAMP,
      numeric_result DOUBLE,
      result_description VARCHAR,
      units VARCHAR,
      range_low DOUBLE,
      range_high DOUBLE,
      encounter_id VARCHAR,
      provider_id VARCHAR,
      order_id VARCHAR
    )"
  )
  DBI::dbExecute(
    con,
    "INSERT INTO stg.lab_results VALUES
      ('1', '1234-5', 'test', DATE '2020-01-01', NULL, NULL, NULL, NULL, '4.8', NULL, NULL, NULL, NULL, NULL, 'a'),
      ('1', '1234-5', 'test', DATE '2020-01-02', NULL, NULL, NULL, NULL, 'Straw', NULL, NULL, NULL, NULL, NULL, 'b'),
      ('1', '1234-5', 'test', DATE '2020-01-03', NULL, NULL, NULL, NULL, 'Plts=163,000 /uL', NULL, NULL, NULL, NULL, NULL, 'c')"
  )
  DBI::dbExecute(con, "CREATE TABLE stg.map_person (member_id VARCHAR, person_id INTEGER)")
  DBI::dbExecute(con, "INSERT INTO stg.map_person VALUES ('1', 101)")
  DBI::dbExecute(
    con,
    "CREATE TABLE stg.map_loinc_measurement (
      loinc_code VARCHAR,
      measurement_concept_id INTEGER,
      measurement_source_concept_id INTEGER
    )"
  )
  DBI::dbExecute(con, "INSERT INTO stg.map_loinc_measurement VALUES ('1234-5', 3001, 12345)")
  DBI::dbExecute(
    con,
    "CREATE TABLE stg.custom_concept_mapping (source_value VARCHAR, domain VARCHAR, concept_id INTEGER)"
  )
  DBI::dbExecute(con, "CREATE TABLE stg.map_units (unit_source_value VARCHAR, unit_concept_id INTEGER)")
  DBI::dbExecute(con, "CREATE TABLE stg.map_visit (encounter_id_source VARCHAR, visit_occurrence_id INTEGER)")
  DBI::dbExecute(con, "CREATE TABLE stg.map_provider (provider_id_source VARCHAR, provider_id INTEGER)")
  DBI::dbExecute(
    con,
    "CREATE TABLE stg.map_measurement_value (result_source_value VARCHAR, value_as_concept_id INTEGER)"
  )
  DBI::dbExecute(con, "INSERT INTO stg.map_measurement_value VALUES ('straw', 763957)")
  DBI::dbExecute(
    con,
    "CREATE TABLE cdm.measurement (
      measurement_id INTEGER,
      person_id INTEGER,
      measurement_concept_id INTEGER,
      measurement_source_concept_id INTEGER,
      measurement_date DATE,
      measurement_datetime TIMESTAMP,
      measurement_type_concept_id INTEGER,
      value_as_number DOUBLE,
      value_as_concept_id INTEGER,
      unit_concept_id INTEGER,
      unit_source_value VARCHAR,
      range_low DOUBLE,
      range_high DOUBLE,
      visit_occurrence_id INTEGER,
      provider_id INTEGER,
      measurement_source_value VARCHAR,
      value_source_value VARCHAR
    )"
  )

  sql_path <- system.file("sql", "50_cdm_clinical", "65_load_measurement_labs.sql", package = "ETLDelphi", mustWork = TRUE)
  sql <- paste(readLines(sql_path, warn = FALSE), collapse = "\n")
  sql <- gsub("\\{measurement_type_labs\\}", "32856", sql)
  sql <- gsub("--[^\n]*", "\n", sql)
  for (stmt in ETLDelphi:::split_sql_statements(sql)) {
    stmt <- trimws(stmt)
    if (!nzchar(stmt)) next
    DBI::dbExecute(con, stmt)
  }

  out <- DBI::dbGetQuery(
    con,
    "SELECT measurement_date, value_as_number, value_as_concept_id, value_source_value
     FROM cdm.measurement
     ORDER BY measurement_date"
  )

  expect_equal(out$value_as_number[1], 4.8)
  expect_true(is.na(out$value_as_concept_id[1]))
  expect_equal(out$value_as_concept_id[2], 763957)
  expect_equal(out$value_source_value[2], "Straw")
  expect_true(is.na(out$value_as_number[3]))
  expect_true(is.na(out$value_as_concept_id[3]))
  expect_equal(out$value_source_value[3], "Plts=163,000 /uL")
})
