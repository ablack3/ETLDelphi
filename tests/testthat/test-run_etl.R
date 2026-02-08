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
