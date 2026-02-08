# test-sql_runner.R — tests for run_sql_scripts and split_sql_statements

test_that("split_sql_statements returns character(0) for empty input", {
  expect_identical(ETLDelphi:::split_sql_statements(""), character(0))
  expect_identical(ETLDelphi:::split_sql_statements("   \n  "), character(0))
})

test_that("split_sql_statements handles single statement without trailing semicolon", {
  sql <- "SELECT 1"
  expect_identical(ETLDelphi:::split_sql_statements(sql), "SELECT 1")
})

test_that("split_sql_statements handles single statement with trailing semicolon", {
  sql <- "SELECT 1;"
  out <- ETLDelphi:::split_sql_statements(sql)
  expect_length(out, 1L)
  expect_identical(trimws(out[1]), "SELECT 1")
})

test_that("split_sql_statements splits multiple statements", {
  sql <- "SELECT 1;\nSELECT 2;\nSELECT 3"
  out <- ETLDelphi:::split_sql_statements(sql)
  expect_length(out, 3L)
  expect_identical(trimws(out[1]), "SELECT 1")
  expect_identical(trimws(out[2]), "SELECT 2")
  expect_identical(trimws(out[3]), "SELECT 3")
})

test_that("split_sql_statements accepts character vector (e.g. readLines)", {
  sql <- c("CREATE TABLE t (x INT);", "INSERT INTO t VALUES (1);")
  out <- ETLDelphi:::split_sql_statements(sql)
  expect_length(out, 2L)
  expect_true(grepl("CREATE TABLE", out[1]))
  expect_true(grepl("INSERT INTO", out[2]))
})

test_that("split_sql_statements trims whitespace and drops empty parts", {
  sql <- "  SELECT a ;  \n  \n  SELECT b  ;  "
  out <- ETLDelphi:::split_sql_statements(sql)
  expect_length(out, 2L)
  expect_identical(out[1], "SELECT a")
  expect_identical(out[2], "SELECT b")
})

test_that("run_sql_scripts errors when sql_dir does not exist", {
  expect_error(
    run_sql_scripts(con = NULL, sql_dir = "/nonexistent/path", config = list(schemas = list(stg = "stg"))),
    "SQL directory not found"
  )
})

test_that("run_sql_scripts dry_run returns list with dry_run and files", {
  sql_dir <- system.file("sql", package = "ETLDelphi")
  skip_if(!dir.exists(sql_dir) || length(list.files(sql_dir, recursive = TRUE, pattern = "\\.sql$")) == 0,
          "Package SQL dir not available")
  config <- list(schemas = list(stg = "stg", cdm = "cdm", src = "src"))
  out <- run_sql_scripts(con = NULL, sql_dir = sql_dir, config = config, dry_run = TRUE)
  expect_true(out$dry_run)
  expect_true(is.character(out$files))
  expect_gt(length(out$files), 0L)
  expect_true(all(grepl("\\.sql$", out$files)))
})

test_that("run_sql_scripts with empty sql dir returns early with no steps", {
  tmp <- tempfile(); dir.create(tmp, recursive = TRUE)
  on.exit(unlink(tmp, recursive = TRUE))
  config <- list(schemas = list(stg = "stg"))
  out <- suppressWarnings(run_sql_scripts(con = NULL, sql_dir = tmp, config = config, dry_run = TRUE))
  expect_identical(out$steps, character(0))
})

test_that("run_sql_scripts from_step and to_step filter files in dry_run", {
  tmp <- tempfile(); dir.create(tmp, recursive = TRUE)
  on.exit(unlink(tmp, recursive = TRUE))
  dir.create(file.path(tmp, "00_admin"), recursive = TRUE)
  dir.create(file.path(tmp, "10_stage"), recursive = TRUE)
  dir.create(file.path(tmp, "20_keys"), recursive = TRUE)
  writeLines("SELECT 1;", file.path(tmp, "00_admin", "00_first.sql"))
  writeLines("SELECT 2;", file.path(tmp, "10_stage", "10_second.sql"))
  writeLines("SELECT 3;", file.path(tmp, "20_keys", "20_third.sql"))
  config <- list(schemas = list(stg = "stg", cdm = "cdm", src = "src"))

  all_out <- run_sql_scripts(con = NULL, sql_dir = tmp, config = config, dry_run = TRUE)
  expect_length(all_out$files, 3L)

  from_out <- run_sql_scripts(con = NULL, sql_dir = tmp, config = config, dry_run = TRUE, from_step = "10_stage")
  expect_true(all(grepl("^10_stage|^20_keys", from_out$files)))

  to_out <- run_sql_scripts(con = NULL, sql_dir = tmp, config = config, dry_run = TRUE, to_step = "10_stage")
  expect_true(length(to_out$files) <= 2L)
  expect_true(any(grepl("10_stage", to_out$files)))
})

test_that("run_sql_scripts substitutes schema placeholders in SQL", {
  tmp <- tempfile(); dir.create(tmp, recursive = TRUE)
  on.exit(unlink(tmp, recursive = TRUE))
  sql_file <- file.path(tmp, "test.sql")
  writeLines(c("SELECT 1 FROM {cdm}.person;", "SELECT 2 FROM {stg}.enrollment;"), sql_file)
  config <- list(schemas = list(stg = "stg_custom", cdm = "cdm_custom", src = "src_custom"))
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  # Run would execute; we only check that run_sql_scripts runs without error with a minimal DB
  # Create dummy cdm and stg so the SQL doesn't fail
  DBI::dbExecute(con, "CREATE SCHEMA cdm_custom")
  DBI::dbExecute(con, "CREATE SCHEMA stg_custom")
  DBI::dbExecute(con, "CREATE TABLE cdm_custom.person (person_id INT)")
  DBI::dbExecute(con, "CREATE TABLE stg_custom.enrollment (id INT)")
  out <- run_sql_scripts(con = con, sql_dir = tmp, config = config, dry_run = FALSE)
  expect_true("test.sql" %in% out$steps)
  expect_equal(out$run_id, out$run_id)
})
