## ----setup, include = FALSE---------------------------------------------------
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>",
  eval = FALSE
)


## ----run-full-----------------------------------------------------------------
library(DBI)
library(duckdb)
library(ETLDelphi)

con <- dbConnect(duckdb::duckdb(), "path/to/your.duckdb")
run_etl(con)
dbDisconnect(con, shutdown = TRUE)


## ----dry-run------------------------------------------------------------------
run_etl(con, dry_run = TRUE)


## ----from-to------------------------------------------------------------------
# From admin through staging
run_etl(con, from_step = "00_admin", to_step = "02_clear")

# From CDM core through end (assumes stg already populated)
run_etl(con, from_step = "40_cdm_core")

# Only QC
run_etl(con, from_step = "90_qc", to_step = "94_required_fields")


## ----custom-------------------------------------------------------------------
run_etl(con, config_path = "/path/to/config.yml")
run_etl(con, sql_dir = "/path/to/sql")

