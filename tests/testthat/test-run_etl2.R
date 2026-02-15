test_that("run_etl", {
  skip_if_not_installed("duckdb")

  # Use prepopulated DuckDB (vocab in cdm, source tables in src). Path from env or default.
  db_path <- Sys.getenv("ETL_TEST_DB", "~/Desktop/delphi.duckdb")
  db_path <- path.expand(db_path)
  skip_if(!file.exists(db_path), "Prepopulated ETL_TEST_DB not found; set ETL_TEST_DB or create ~/Desktop/delphi.duckdb")

  con <- tryCatch(
    DBI::dbConnect(duckdb::duckdb(), db_path),
    error = function(e) NULL
  )
  skip_if(is.null(con), "Could not connect to prepopulated DB (file may be locked by another process)")

  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Run the ETL with default config (codeToRun.R now passes config list in script)
  out <- run_etl(con = con, config = NULL)

  expect_true(is.list(out))
  expect_true(length(out$steps) >= 1L)
})
