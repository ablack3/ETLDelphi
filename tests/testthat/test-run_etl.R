# test-run_etl.R — tests for run_etl and %||%

test_that("%||% returns LHS when not null", {
  `%||%` <- ETLDelphi:::`%||%`
  expect_identical("a" %||% "b", "a")
  expect_identical(1L %||% 2L, 1L)
  expect_identical(FALSE %||% TRUE, FALSE)
})

test_that("%||% returns RHS when LHS is NULL", {
  `%||%` <- ETLDelphi:::`%||%`
  expect_identical(NULL %||% "default", "default")
  expect_identical(NULL %||% 42L, 42L)
})

test_that("run_etl errors when config file missing and config not provided", {
  expect_error(
    run_etl(con = NULL, config_path = "/nonexistent/config.yml", config = NULL),
    "Config file not found"
  )
})

test_that("run_etl uses config list when provided and ignores config_path", {
  config <- list(schemas = list(stg = "stg", cdm = "cdm", src = "src"))
  sql_dir <- system.file("sql", package = "ETLDelphi")
  skip_if(!dir.exists(sql_dir), "Package SQL dir not available")
  # dry_run so we don't need a real connection
  out <- run_etl(con = NULL, config_path = "/bad/path.yml", config = config, sql_dir = sql_dir, dry_run = TRUE)
  expect_true(out$dry_run)
  expect_true(is.character(out$files))
})

test_that("run_etl uses default config path when config_path is NULL and config is NULL", {
  # Without installing the package, system.file may return "" for sql and extdata
  config_path <- system.file("extdata", "config.yml", package = "ETLDelphi")
  skip_if(!file.exists(config_path), "Package extdata config not available")
  sql_dir <- system.file("sql", package = "ETLDelphi")
  skip_if(!dir.exists(sql_dir), "Package SQL dir not available")
  out <- run_etl(con = NULL, config_path = NULL, config = NULL, sql_dir = NULL, dry_run = TRUE)
  expect_true(out$dry_run)
})

test_that("run_etl errors when sql_dir does not exist and not using package default", {
  config <- list(schemas = list(stg = "stg", cdm = "cdm", src = "src"))
  expect_error(
    run_etl(con = NULL, config = config, sql_dir = "/nonexistent/sql/dir", dry_run = TRUE),
    "SQL directory not found"
  )
})

test_that("run_etl passes from_step and to_step to run_sql_scripts in dry_run", {
  config <- list(schemas = list(stg = "stg", cdm = "cdm", src = "src"))
  tmp <- tempfile(); dir.create(tmp, recursive = TRUE)
  on.exit(unlink(tmp, recursive = TRUE))
  dir.create(file.path(tmp, "00_admin"), recursive = TRUE)
  writeLines("SELECT 1;", file.path(tmp, "00_admin", "00_a.sql"))
  out <- run_etl(con = NULL, config = config, sql_dir = tmp, dry_run = TRUE, from_step = "00_admin", to_step = "00_admin")
  expect_true(out$dry_run)
  expect_true(any(grepl("00_admin", out$files)))
})

test_that("run_etl runs to completion on a minimal DuckDB (DDL + empty src tables)", {
  skip_if_not_installed("duckdb")
  db_path <- tempfile(fileext = ".duckdb")
  on.exit({
    if (file.exists(db_path)) unlink(db_path)
  }, add = TRUE)

  con <- DBI::dbConnect(duckdb::duckdb(), db_path)

  # Run CDM DDL so cdm.* tables exist (no vocabulary data)
  DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS cdm")
  ddl_path <- system.file("omop_cdm_specification", "OMOPCDM_duckdb_5.4_ddl.sql", package = "ETLDelphi", mustWork = TRUE)
  sql <- readLines(ddl_path, warn = FALSE)
  sql <- paste(sql, collapse = "\n")
  sql <- gsub("@cdmDatabaseSchema", "cdm", sql, fixed = TRUE)
  sql <- gsub("--[^\n]*", "\n", sql)
  statements <- ETLDelphi:::split_sql_statements(sql)
  for (stmt in statements) {
    stmt <- trimws(stmt)
    if (nchar(stmt) == 0L) next
    DBI::dbExecute(con, stmt)
  }

  # Create src schema and minimal empty tables (columns expected by stage scripts)
  DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS src")
  empty_select <- function(cols) {
    parts <- paste0('CAST(NULL AS VARCHAR) AS "', cols, '"', collapse = ", ")
    paste0("SELECT ", parts, " WHERE 1=0")
  }
  src_tables <- list(
    enrollment = c("Member_ID", "Member_SSN", "Name_First", "MI", "Name_Last", "Title", "DOB", "Gender", "Race", "Address_Line_1", "Address_Line_2", "City", "State", "Zip_Code"),
    provider = c("Provider_ID", "NPI", "Name", "Specialty", "DOB", "Sex", "Facility_Name", "Location"),
    encounter = c("Encounter_ID", "Member_ID", "Appt_Type", "Provider_ID", "Clinic_ID", "Encounter_DateTime", "Clinic_Type", "SOAP_Note"),
    death = c("Member_ID", "DOD"),
    problem = c("Member_ID", "Problem_Code", "Problem_Description", "Problem_Type", "Onset_Date", "Resolution_Date", "Provider_ID", "Encounter_ID"),
    medication_orders = c("Member_ID", "Order_ID", "Drug_Name", "Drug_NDC", "Order_Date", "Last_Filled_Date", "Dose", "Qty_Ordered", "Refills", "Sig", "Route", "Units", "Order_Provider_ID", "Encounter_ID"),
    medication_fulfillment = c("Order_ID", "Dispense_Date", "Dispense_Qty", "Days_Of_Supply", "Fill_No", "Encounter_ID"),
    current_medications = c("Member_ID", "Last_Filled_Date", "Drug_Name", "Sig", "Refills", "Days_Of_Supply", "Order_ID", "Encounter_ID"),
    immunization = c("Member_ID", "Vaccine_CVX", "Vaccine_Name", "Vaccination_Date", "Dose", "Units", "Route", "Lot_Number", "Provider_ID", "Encounter_ID"),
    lab_orders = c("Order_ID", "Order_Date", "Patient_ID", "Test_LOINC", "Test_Name", "Encounter_ID"),
    lab_results = c("Member_ID", "Order_ID", "Test_LOINC", "Test_Name", "Date_Collected", "Date_Resulted", "Numeric_Result", "Units", "Result_Description", "Reference_Range", "Provider_ID", "Encounter_ID"),
    vital_sign = c("Member_ID", "Encounter_ID", "Encounter_Date", "Height", "Height_Units", "Weight", "Weight_Units", "SystolicBP", "DiastolicBP", "Pulse", "Respiration", "Temperature", "Temperature_Units"),
    allergy = c("Member_ID", "Allergen", "Drug_Code", "Drug_Vocab", "Allergy_Type", "Onset_Date", "Reaction", "Severity_Description"),
    therapy_orders = c("Member_ID", "Order_ID", "Code", "Name", "Target_Area", "Vocabulary", "Encounter_ID"),
    therapy_actions = c("Member_ID", "Order_ID", "Code", "Name", "Result", "Target_Area", "Vocabulary", "Encounter_ID")
  )
  for (tbl in names(src_tables)) {
    DBI::dbExecute(con, paste0('CREATE TABLE src.', tbl, ' AS ', empty_select(src_tables[[tbl]])))
  }

  # Same pattern as extras/codeToRun.R: use default config when path is NA or empty
  config_path <- NULL
  out <- run_etl(
    con = con,
    config_path = if (!is.null(config_path) && !is.na(config_path) && nzchar(config_path)) config_path else NULL
  )
  expect_true(is.list(out))
  expect_true(length(out$steps) >= 1L)

  DBI::dbDisconnect(con, shutdown = TRUE)
})

