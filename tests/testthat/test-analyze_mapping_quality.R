# test-analyze_mapping_quality.R — tests for analyze_mapping_quality

test_that("analyze_mapping_quality runs and writes expected CSVs", {
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

  output_dir <- tempfile()
  on.exit(if (dir.exists(output_dir)) unlink(output_dir, recursive = TRUE), add = TRUE)

  config <- list(schemas = list(stg = "stg", cdm = "cdm"))
  out <- analyze_mapping_quality(con, output_dir = output_dir, config = config)

  expect_true(is.list(out))
  expect_identical(out$output_dir, output_dir)
  expect_true(is.character(out$files))
  expect_true(length(out$files) >= 8L)

  for (f in out$files) {
    expect_true(file.exists(f), info = paste("File should exist:", f))
  }

  # Spot-check first CSV has expected structure
  csv1 <- read.csv(out$files[1], stringsAsFactors = FALSE)
  expect_true(is.data.frame(csv1))
})
