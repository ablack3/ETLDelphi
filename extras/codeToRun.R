# =============================================================================
# codeToRun.R — Load ETLDelphi and run the full ETL with parameterized paths
# =============================================================================
# Set the paths and config below (or pass via environment / command line), then
# source this file or run in R to create the DuckDB, load vocabulary and
# Delphi source data, and execute the ETL.
# =============================================================================

# --- Path parameters (edit these or set via Sys.setenv / command line) -------

# Directory containing vocabulary CSV/TSV files (e.g. CONCEPT.csv, VOCABULARY.csv)
vocabulary_dir <- "vocabulary_download_v5"

# Directory containing Delphi source CSV files (e.g. enrollment.csv, encounter.csv)
delphi_source_dir <- "delphi100k"

# Path to the output DuckDB database file (created or overwritten)
duckdb_path <- "~/Desktop/delphi.duckdb"

file.remove(duckdb_path)

# Optional: vocabulary file delimiter ("," for CSV, "\t" for TSV)
vocab_delimiter <- "\t"

# --- ETL config (schemas, type concept IDs, optional mapping paths) ----------
# Edit this list to override defaults. NULL paths use package defaults.
config <- list(
  schemas = list(src = "src", stg = "stg", cdm = "main"),
  concept_id_unknown = 0L,
  date_formats = c(
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%d-%b-%Y", "%Y/%m/%d"
  ),
  type_concept_ids = list(
    visit_type_concept_id = 44818517L,
    default_visit_concept_id = 44813942L,
    condition_type_concept_id = 32818L,
    drug_type_orders = 38000177L,
    drug_type_dispensed = 38000230L,
    drug_type_immunization = 38000280L,
    measurement_type_vitals = 32817L,
    measurement_type_labs = 32827L,
    observation_type_allergy = 32859L,
    note_type_concept_id = 44813942L,
    note_class_concept_id = 44814639L,
    language_concept_id = 4180186L,
    encoding_concept_id = 44815386L,
    procedure_type_concept_id = 38000268L,
    period_type_concept_id = 32821L,
    death_type_concept_id = 32817L
  ),
  prefer_fulfillment = FALSE,
  drug_name_mapping_path = NULL,   # NULL = use package default drug_name_to_concept.csv
  custom_mapping_path = NULL,     # NULL = use package default custom_concept_mapping.csv
  custom_ndc_mapping_path = NULL  # NULL = use package default custom_ndc_mapping.csv
)

if (file.exists(duckdb_path)) file.remove(duckdb_path)

# --- Load package and connect -------------------------------------------------

library(DBI)
library(duckdb)
library(ETLDelphi)

# Resolve paths relative to current working directory if not absolute
if (!dir.exists(vocabulary_dir)) {
  stop("Vocabulary directory not found: ", vocabulary_dir)
}
if (!dir.exists(delphi_source_dir)) {
  stop("Delphi source directory not found: ", delphi_source_dir)
}

vocabulary_dir <- normalizePath(vocabulary_dir, winslash = "/")
delphi_source_dir <- normalizePath(delphi_source_dir, winslash = "/")
duckdb_path <- normalizePath(duckdb_path, mustWork = FALSE, winslash = "/")

# --- 1. Initialize Database, CDM DDL, and load vocabulary -------------------------------

con <- ETLDelphi::init_vocabulary_db(
  duckdb_path,
  vocabulary_dir = vocabulary_dir,
  cdm_schema = config$schemas$cdm
)

DBI::dbGetQuery(con, "select * from main.concept_relationship limit 10")
DBI::dbGetQuery(con, "select * from main.concept limit 10")

# cdm <- CDMConnector::cdmFromCon(con, "main")
# cdm$concept
#
# cdm$vocabulary


# --- 2. Create src schema and load Delphi CSVs -------------------------------

ETLDelphi::init_source_data(con, delphi_source_dir)

# --- 3. Run the ETL ----------------------------------------------------------

DBI::dbDisconnect(con, shutdown = T)

con <- DBI::dbConnect(duckdb::duckdb(), "~/Desktop/delphi.duckdb")

ETLDelphi::run_etl(con = con, config = config)

unlink(here::here("inst/shiny/mapping_quality/mapping_quality_results"), recursive = T)
ETLDelphi::analyze_mapping_quality(con, output_dir = "inst/shiny/mapping_quality/mapping_quality_results")
list.files("inst/shiny/mapping_quality/mapping_quality_results")
run_mapping_quality_app(results_dir = here::here("inst/shiny/mapping_quality/mapping_quality_results"))


# measurement_value	no abnormal skin or mole present	70,549
# measurement_value	no lumps noted	69,582
# measurement_value	no lumps noticed	38,357
# measurement_value	uterus and ovaries are normal in size and location	35,321
# measurement_value	no abnormal cervical cells	35,320
# measurement_value	negative for HPV 16 & 18	21,633
# drug	59630*70248	7,153
# measurement_value	Platelet count = 200,000/uL

devtools::load_all()
rebuild_custom_mappings_from_log(dry_run = TRUE)  # preview
rebuild_custom_mappings_from_log()                  # write CSVs
run_etl(con, from_step = "30_vocab")               # re-run with pattern mapping


df <- readr::read_csv("mapping_improvement_log.csv")

# Create LLM generated custom mappings
improve_mappings(
  con,
  config = config,
  domains = "measurement_value",
  force_retry = "fail",
  dry_run = TRUE,
  limit = 10,
  confidence_threshold = .7,
  provider = "openai"
)

improve_mappings(
  con,
  config = config,
  # domains = "drug",
  limit = 10,
  confidence_threshold = .7,
  provider = "openai"
)



# Export unmapped units, measurement values, and drugs for manual mapping (see extras/UNMAPPED_UNITS.md)
ETLDelphi::export_unmapped_units(con, output_path = "unmapped_units.csv")
ETLDelphi::export_unmapped_measurement_values(con, output_path = "unmapped_measurement_values.csv")
ETLDelphi::export_unmapped_drugs(con, output_path = "unmapped_drugs.csv")


DBI::dbDisconnect(con, shutdown = TRUE)

# --- 3. Run Achilles ----------------------------------------------------------

cd <- DatabaseConnector::createConnectionDetails(
  dbms = "duckdb",
  server = "~/Desktop/delphi.duckdb"
)

con <- DatabaseConnector::connect(cd)
DatabaseConnector::querySql(con, "create schema if not exists scratch")
DatabaseConnector::querySql(con, "create schema if not exists achilles")
DatabaseConnector::querySql(con, "create schema if not exists achilles")
DatabaseConnector::disconnect(con)

r <- Achilles::achilles(
  connectionDetails = cd,
  cdmDatabaseSchema = "main",
  resultsDatabaseSchema = "achilles",
  scratchDatabaseSchema = "scratch",
  dropScratchTables = T,
  optimizeAtlasCache = T,
  defaultAnalysesOnly = F
)


# --- 3. Run DQD ----------------------------------------------------------

DatabaseConnector::querySql(con, "create schema if not exists dqd")
DataQualityDashboard::executeDqChecks(
  connectionDetails = cd,
  cdmDatabaseSchema = "main",
  resultsDatabaseSchema = "dqd",
  cdmSourceName = "Delphi",
  outputFolder = here::here("DQD_results"),
  tablesToExclude = c("COHORT", "COHORT_DEFINITION"),
  writeToTable = FALSE,
  verboseMode = TRUE,
  cdmVersion = "5.4"
)
list.files( here::here("DQD_results"))
DataQualityDashboard::viewDqDashboard(here::here("DQD_results", "delphi-2m-20260224193532.json"))



library(CDMConnector)

con <- DBI::dbConnect(duckdb::duckdb(), "~/Desktop/delphi.duckdb")

cdm <- cdmFromCon(
  con,
  cdmSchema = "main",
  writeSchema = "scratch",
  achillesSchema = "achilles"
)

snapshot(cdm) %>%
  tidyr::gather()

DBI::dbListTables(con)

DBI::dbGetQuery(con, "
select table_schema, table_name
from information_schema.tables
where table_type in ('BASE TABLE','VIEW')
order by 1,2;")

DBI::dbListTables(con)

# DBI::dbExecute(con, "DROP SCHEMA src CASCADE;")
# DBI::dbExecute(con, "DROP SCHEMA stg CASCADE;")
message("ETL complete. DuckDB output: ", duckdb_path)

