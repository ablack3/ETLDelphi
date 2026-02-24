#' Analyze data mapping quality and save results to CSV
#'
#' Runs quality checks on the ETL mapping: unmapped concepts (concept_id 0),
#' record and person count comparisons, mapping cardinality (one-to-many,
#' many-to-one), and data not mapped to OMOP (reject tables, unmapped rows).
#' Writes one CSV per metric set into \code{output_dir}, plus a
#' \code{mapping_quality.json} file consumed by the static HTML dashboard.
#'
#' @param con DBI connection to the DuckDB database (stg and cdm schemas must exist).
#' @param output_dir Directory where CSV and JSON files will be written. Created if missing.
#' @param config Optional list with \code{schemas$stg} and \code{schemas$cdm}; if NULL, uses \code{stg} and \code{cdm}.
#' @return Invisible list with \code{output_dir} and \code{files} (paths to written CSV and JSON files).
#' @export
analyze_mapping_quality <- function(con, output_dir = "mapping_quality_results", config = NULL) {
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }

  out_path <- function(name) file.path(output_dir, name)

  # Run all queries via shared internal helper

  data <- run_mapping_quality_queries(con, config)

  # Write each dataset to CSV
  csv_map <- list(
    "01_unmapped_concepts_by_table.csv"   = data$unmapped_concepts,
    "02_top_unmapped_source_values.csv"    = data$top_unmapped_source_values,
    "03_record_counts_by_table.csv"        = data$record_counts,
    "04_person_count_comparison.csv"        = data$person_count,
    "05_one_to_many_mappings.csv"           = data$one_to_many,
    "06_many_to_one_mappings.csv"           = data$many_to_one,
    "07_reject_table_row_counts.csv"        = data$reject_counts,
    "08_data_not_mapped_summary.csv"        = data$summary,
    "09_domain_conformance.csv"             = data$domain_conformance,
    "10_domain_routing_log.csv"             = data$routing_log
  )

  written <- character(0)
  for (fname in names(csv_map)) {
    p <- out_path(fname)
    write.csv(csv_map[[fname]], p, row.names = FALSE)
    written <- c(written, p)
  }

  # Write JSON for the static HTML dashboard
  json_path <- out_path("mapping_quality.json")
  data$generated_at <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  json <- jsonlite::toJSON(data, pretty = TRUE, auto_unbox = TRUE, dataframe = "rows")
  writeLines(json, json_path)
  written <- c(written, json_path)
  cli::cli_alert_success("Wrote {json_path}")

  invisible(list(output_dir = output_dir, files = written))
}


# Internal: run all mapping quality queries and return named list of data.frames.
# Shared by analyze_mapping_quality() (CSV output) and export_mapping_quality_json() (JSON output).
run_mapping_quality_queries <- function(con, config = NULL) {
  stg <- resolve_schema(config, "stg")
  cdm <- resolve_schema(config, "cdm")

  # ----- 1. Unmapped concepts (concept_id = 0) per CDM table -----
  q_unmapped <- list(
    condition_occurrence = glue::glue(
      "SELECT '{cdm}.condition_occurrence' AS cdm_table, 'condition_concept_id' AS concept_column,",
      " COUNT(*) AS total_rows, SUM(CASE WHEN condition_concept_id = 0 THEN 1 ELSE 0 END) AS unmapped_count,",
      " 100.0 * SUM(CASE WHEN condition_concept_id = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS unmapped_pct ",
      "FROM \"{cdm}\".condition_occurrence"
    ),
    drug_exposure = glue::glue(
      "SELECT '{cdm}.drug_exposure' AS cdm_table, 'drug_concept_id' AS concept_column,",
      " COUNT(*) AS total_rows, SUM(CASE WHEN drug_concept_id = 0 THEN 1 ELSE 0 END) AS unmapped_count,",
      " 100.0 * SUM(CASE WHEN drug_concept_id = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS unmapped_pct ",
      "FROM \"{cdm}\".drug_exposure"
    ),
    measurement = glue::glue(
      "SELECT '{cdm}.measurement' AS cdm_table, 'measurement_concept_id' AS concept_column,",
      " COUNT(*) AS total_rows, SUM(CASE WHEN measurement_concept_id = 0 THEN 1 ELSE 0 END) AS unmapped_count,",
      " 100.0 * SUM(CASE WHEN measurement_concept_id = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS unmapped_pct ",
      "FROM \"{cdm}\".measurement"
    ),
    procedure_occurrence = glue::glue(
      "SELECT '{cdm}.procedure_occurrence' AS cdm_table, 'procedure_concept_id' AS concept_column,",
      " COUNT(*) AS total_rows, SUM(CASE WHEN procedure_concept_id = 0 THEN 1 ELSE 0 END) AS unmapped_count,",
      " 100.0 * SUM(CASE WHEN procedure_concept_id = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS unmapped_pct ",
      "FROM \"{cdm}\".procedure_occurrence"
    ),
    observation = glue::glue(
      "SELECT '{cdm}.observation' AS cdm_table, 'observation_concept_id' AS concept_column,",
      " COUNT(*) AS total_rows, SUM(CASE WHEN observation_concept_id = 0 THEN 1 ELSE 0 END) AS unmapped_count,",
      " 100.0 * SUM(CASE WHEN observation_concept_id = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS unmapped_pct ",
      "FROM \"{cdm}\".observation"
    ),
    measurement_unit = glue::glue(
      "SELECT '{cdm}.measurement' AS cdm_table, 'unit_concept_id' AS concept_column,",
      " COUNT(*) AS total_rows, SUM(CASE WHEN unit_concept_id = 0 THEN 1 ELSE 0 END) AS unmapped_count,",
      " 100.0 * SUM(CASE WHEN unit_concept_id = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS unmapped_pct ",
      "FROM \"{cdm}\".measurement WHERE unit_source_value IS NOT NULL AND TRIM(unit_source_value) <> ''"
    ),
    measurement_value_as_concept = glue::glue(
      "SELECT '{cdm}.measurement' AS cdm_table, 'value_as_concept_id' AS concept_column,",
      " COUNT(*) AS total_rows, SUM(CASE WHEN value_as_concept_id = 0 THEN 1 ELSE 0 END) AS unmapped_count,",
      " 100.0 * SUM(CASE WHEN value_as_concept_id = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS unmapped_pct ",
      "FROM \"{cdm}\".measurement WHERE value_as_concept_id IS NOT NULL"
    )
  )

  df_unmapped <- tryCatch({
    res <- lapply(q_unmapped, function(sql) DBI::dbGetQuery(con, sql))
    do.call(rbind, res)
  }, error = function(e) {
    data.frame(cdm_table = character(), concept_column = character(), total_rows = integer(),
               unmapped_count = integer(), unmapped_pct = numeric(), stringsAsFactors = FALSE)
  })

  # ----- 2. Top source values for unmapped (concept_id = 0) -----
  q_top_unmapped <- list(
    condition = glue::glue(
      "SELECT condition_source_value AS source_value, COUNT(*) AS record_count ",
      "FROM \"{cdm}\".condition_occurrence WHERE condition_concept_id = 0 AND condition_source_value IS NOT NULL ",
      "GROUP BY condition_source_value ORDER BY record_count DESC LIMIT 500"
    ),
    drug = glue::glue(
      "SELECT drug_source_value AS source_value, COUNT(*) AS record_count ",
      "FROM \"{cdm}\".drug_exposure WHERE drug_concept_id = 0 AND drug_source_value IS NOT NULL ",
      "GROUP BY drug_source_value ORDER BY record_count DESC LIMIT 500"
    ),
    measurement = glue::glue(
      "SELECT measurement_source_value AS source_value, COUNT(*) AS record_count ",
      "FROM \"{cdm}\".measurement WHERE measurement_concept_id = 0 AND measurement_source_value IS NOT NULL ",
      "GROUP BY measurement_source_value ORDER BY record_count DESC LIMIT 500"
    ),
    procedure = glue::glue(
      "SELECT procedure_source_value AS source_value, COUNT(*) AS record_count ",
      "FROM \"{cdm}\".procedure_occurrence WHERE procedure_concept_id = 0 AND procedure_source_value IS NOT NULL ",
      "GROUP BY procedure_source_value ORDER BY record_count DESC LIMIT 500"
    ),
    observation = glue::glue(
      "SELECT observation_source_value AS source_value, COUNT(*) AS record_count ",
      "FROM \"{cdm}\".observation WHERE observation_concept_id = 0 AND observation_source_value IS NOT NULL ",
      "GROUP BY observation_source_value ORDER BY record_count DESC LIMIT 500"
    ),
    measurement_unit = glue::glue(
      "SELECT unit_source_value AS source_value, COUNT(*) AS record_count ",
      "FROM \"{cdm}\".measurement WHERE unit_concept_id = 0 AND unit_source_value IS NOT NULL ",
      "GROUP BY unit_source_value ORDER BY record_count DESC LIMIT 500"
    ),
    measurement_value = glue::glue(
      "SELECT value_source_value AS source_value, COUNT(*) AS record_count ",
      "FROM \"{cdm}\".measurement WHERE value_as_concept_id = 0 AND value_source_value IS NOT NULL ",
      "GROUP BY value_source_value ORDER BY record_count DESC LIMIT 500"
    )
  )

  top_unmapped_list <- list()
  for (nm in names(q_top_unmapped)) {
    tryCatch({
      top_unmapped_list[[nm]] <- cbind(domain = nm, DBI::dbGetQuery(con, q_top_unmapped[[nm]]))
    }, error = function(e) NULL)
  }
  df_top <- if (length(top_unmapped_list) > 0) do.call(rbind, top_unmapped_list) else NULL
  if (is.null(df_top) || nrow(df_top) == 0) {
    df_top <- data.frame(domain = character(), source_value = character(), record_count = integer(), stringsAsFactors = FALSE)
  }

  # ----- 3. Record count comparison: source/stg vs CDM -----
  stg_tables <- c(
    "enrollment", "provider", "encounter", "death", "problem", "medication_orders",
    "medication_fulfillment", "immunization", "lab_results", "vital_sign", "allergy",
    "therapy_orders", "therapy_actions", "current_medications", "lab_orders"
  )
  cdm_tables <- c(
    "person", "visit_occurrence", "condition_occurrence", "drug_exposure", "measurement",
    "observation", "procedure_occurrence", "death", "observation_period"
  )

  get_count <- function(schema, table) {
    full <- paste0('"', schema, '"."', table, '"')
    if (!table_exists(con, schema, table)) return(NA_integer_)
    tryCatch({
      as.integer(DBI::dbGetQuery(con, paste("SELECT COUNT(*) AS n FROM", full))$n)
    }, error = function(e) NA_integer_)
  }

  rows_stg <- data.frame(
    table_name = paste0("stg.", stg_tables),
    row_count = vapply(stg_tables, function(t) get_count(stg, t), integer(1)),
    layer = "stg",
    stringsAsFactors = FALSE
  )
  rows_cdm <- data.frame(
    table_name = paste0("cdm.", cdm_tables),
    row_count = vapply(cdm_tables, function(t) get_count(cdm, t), integer(1)),
    layer = "cdm",
    stringsAsFactors = FALSE
  )
  df_record_counts <- rbind(rows_stg, rows_cdm)

  # ----- 4. Person count comparison -----
  q_person_stg <- glue::glue(
    "SELECT COUNT(DISTINCT member_id) AS distinct_members FROM \"{stg}\".enrollment ",
    "WHERE member_id IS NOT NULL AND TRIM(member_id) <> ''"
  )
  q_person_cdm <- glue::glue("SELECT COUNT(*) AS person_count FROM \"{cdm}\".person")
  person_stg <- tryCatch(DBI::dbGetQuery(con, q_person_stg)$distinct_members, error = function(e) NA_integer_)
  person_cdm <- tryCatch(DBI::dbGetQuery(con, q_person_cdm)$person_count, error = function(e) NA_integer_)
  df_person <- data.frame(
    source = c("stg_enrollment_distinct_member_id", "cdm_person"),
    count = c(person_stg, person_cdm),
    stringsAsFactors = FALSE
  )

  # ----- 5. One-to-many: one source key maps to multiple concept_ids -----
  one_to_many <- list()
  if (table_exists(con, stg, "map_condition")) {
    q <- glue::glue(
      "SELECT problem_code AS source_key, COUNT(DISTINCT condition_concept_id) AS target_count ",
      "FROM \"{stg}\".map_condition GROUP BY problem_code HAVING COUNT(DISTINCT condition_concept_id) > 1"
    )
    tryCatch({
      one_to_many[["condition"]] <- cbind(mapping = "map_condition", DBI::dbGetQuery(con, q))
    }, error = function(e) NULL)
  }
  if (table_exists(con, stg, "map_drug_order")) {
    q <- glue::glue(
      "SELECT COALESCE(CAST(drug_ndc_normalized AS VARCHAR), '') || '|' || COALESCE(drug_name, '') AS source_key, ",
      "COUNT(DISTINCT drug_concept_id) AS target_count ",
      "FROM \"{stg}\".map_drug_order GROUP BY drug_ndc_normalized, drug_name HAVING COUNT(DISTINCT drug_concept_id) > 1"
    )
    tryCatch({
      one_to_many[["drug_order"]] <- cbind(mapping = "map_drug_order", DBI::dbGetQuery(con, q))
    }, error = function(e) NULL)
  }
  if (table_exists(con, stg, "map_loinc_measurement")) {
    q <- glue::glue(
      "SELECT loinc_code AS source_key, COUNT(DISTINCT measurement_concept_id) AS target_count ",
      "FROM \"{stg}\".map_loinc_measurement GROUP BY loinc_code HAVING COUNT(DISTINCT measurement_concept_id) > 1"
    )
    tryCatch({
      one_to_many[["loinc_measurement"]] <- cbind(mapping = "map_loinc_measurement", DBI::dbGetQuery(con, q))
    }, error = function(e) NULL)
  }
  if (table_exists(con, stg, "map_therapy")) {
    q <- glue::glue(
      "SELECT code AS source_key, COUNT(DISTINCT procedure_concept_id) AS target_count ",
      "FROM \"{stg}\".map_therapy GROUP BY code, vocabulary HAVING COUNT(DISTINCT procedure_concept_id) > 1"
    )
    tryCatch({
      one_to_many[["therapy"]] <- cbind(mapping = "map_therapy", DBI::dbGetQuery(con, q))
    }, error = function(e) NULL)
  }

  df_one_to_many <- if (length(one_to_many) > 0) do.call(rbind, one_to_many) else NULL
  if (is.null(df_one_to_many) || nrow(df_one_to_many) == 0) {
    df_one_to_many <- data.frame(mapping = character(), source_key = character(), target_count = integer(), stringsAsFactors = FALSE)
  }

  # ----- 6. Many-to-one: multiple source keys map to same concept_id -----
  many_to_one <- list()
  if (table_exists(con, stg, "map_condition")) {
    q <- glue::glue(
      "SELECT condition_concept_id AS concept_id, COUNT(DISTINCT problem_code) AS source_key_count ",
      "FROM \"{stg}\".map_condition WHERE condition_concept_id <> 0 ",
      "GROUP BY condition_concept_id HAVING COUNT(DISTINCT problem_code) > 1"
    )
    tryCatch({
      many_to_one[["condition"]] <- cbind(mapping = "map_condition", DBI::dbGetQuery(con, q))
    }, error = function(e) NULL)
  }
  if (table_exists(con, stg, "map_drug_order")) {
    q <- glue::glue(
      "SELECT drug_concept_id AS concept_id, COUNT(*) AS source_key_count ",
      "FROM \"{stg}\".map_drug_order WHERE drug_concept_id <> 0 GROUP BY drug_concept_id HAVING COUNT(*) > 1"
    )
    tryCatch({
      many_to_one[["drug_order"]] <- cbind(mapping = "map_drug_order", DBI::dbGetQuery(con, q))
    }, error = function(e) NULL)
  }
  if (table_exists(con, stg, "map_loinc_measurement")) {
    q <- glue::glue(
      "SELECT measurement_concept_id AS concept_id, COUNT(DISTINCT loinc_code) AS source_key_count ",
      "FROM \"{stg}\".map_loinc_measurement WHERE measurement_concept_id <> 0 ",
      "GROUP BY measurement_concept_id HAVING COUNT(DISTINCT loinc_code) > 1"
    )
    tryCatch({
      many_to_one[["loinc_measurement"]] <- cbind(mapping = "map_loinc_measurement", DBI::dbGetQuery(con, q))
    }, error = function(e) NULL)
  }
  if (table_exists(con, stg, "map_therapy")) {
    q <- glue::glue(
      "SELECT procedure_concept_id AS concept_id, COUNT(DISTINCT code) AS source_key_count ",
      "FROM \"{stg}\".map_therapy WHERE procedure_concept_id <> 0 GROUP BY procedure_concept_id HAVING COUNT(DISTINCT code) > 1"
    )
    tryCatch({
      many_to_one[["therapy"]] <- cbind(mapping = "map_therapy", DBI::dbGetQuery(con, q))
    }, error = function(e) NULL)
  }

  df_many_to_one <- if (length(many_to_one) > 0) do.call(rbind, many_to_one) else NULL
  if (is.null(df_many_to_one) || nrow(df_many_to_one) == 0) {
    df_many_to_one <- data.frame(mapping = character(), concept_id = integer(), source_key_count = integer(), stringsAsFactors = FALSE)
  }

  # ----- 7. Data not mapped to OMOP: reject tables + summary -----
  reject_table_names <- c(
    "reject_enrollment_dates", "reject_provider", "reject_encounter", "reject_death", "reject_problem",
    "reject_med_orders", "reject_med_fulfillment", "reject_current_meds", "reject_immunization",
    "reject_lab_orders", "reject_lab_results", "reject_vital_sign", "reject_allergy",
    "reject_visit_missing_person", "reject_fulfillment_no_order", "reject_person_missing_dob",
    "reject_condition_missing_person", "reject_measurement_labs_missing", "reject_observation_allergy_missing",
    "reject_procedure_missing", "reject_death_load"
  )
  df_reject_counts <- data.frame(
    reject_table = character(),
    row_count = integer(),
    stringsAsFactors = FALSE
  )
  for (tbl in reject_table_names) {
    if (table_exists(con, stg, tbl)) {
      n <- tryCatch({
        as.integer(DBI::dbGetQuery(con, glue::glue('SELECT COUNT(*) AS n FROM "{stg}"."{tbl}"'))$n)
      }, error = function(e) NA_integer_)
      df_reject_counts <- rbind(df_reject_counts, data.frame(reject_table = tbl, row_count = n, stringsAsFactors = FALSE))
    }
  }

  # Summary: total unmapped records (concept_id = 0) and total rejected
  total_rejected <- sum(df_reject_counts$row_count, na.rm = TRUE)
  total_unmapped_cdm <- tryCatch({
    q <- glue::glue(
      "SELECT ",
      "(SELECT SUM(CASE WHEN condition_concept_id = 0 THEN 1 ELSE 0 END) FROM \"{cdm}\".condition_occurrence) + ",
      "(SELECT SUM(CASE WHEN drug_concept_id = 0 THEN 1 ELSE 0 END) FROM \"{cdm}\".drug_exposure) + ",
      "(SELECT SUM(CASE WHEN measurement_concept_id = 0 THEN 1 ELSE 0 END) FROM \"{cdm}\".measurement) + ",
      "(SELECT SUM(CASE WHEN procedure_concept_id = 0 THEN 1 ELSE 0 END) FROM \"{cdm}\".procedure_occurrence) + ",
      "(SELECT SUM(CASE WHEN observation_concept_id = 0 THEN 1 ELSE 0 END) FROM \"{cdm}\".observation) AS total"
    )
    DBI::dbGetQuery(con, q)$total
  }, error = function(e) NA_integer_)
  df_not_mapped_summary <- data.frame(
    metric = c("total_rows_in_reject_tables", "total_cdm_records_with_concept_id_0"),
    value = c(total_rejected, total_unmapped_cdm),
    stringsAsFactors = FALSE
  )

  # ----- 9. Domain conformance (concept domain_id vs CDM table) -----
  domain_conf_queries <- list(
    condition_occurrence = glue::glue(
      "SELECT 'condition_occurrence' AS cdm_table, 'Condition' AS expected_domain, c.domain_id AS concept_domain, COUNT(*) AS record_count ",
      "FROM \"{cdm}\".condition_occurrence co ",
      "JOIN \"{cdm}\".concept c ON c.concept_id = co.condition_concept_id ",
      "WHERE co.condition_concept_id <> 0 GROUP BY c.domain_id"
    ),
    drug_exposure = glue::glue(
      "SELECT 'drug_exposure' AS cdm_table, 'Drug' AS expected_domain, c.domain_id AS concept_domain, COUNT(*) AS record_count ",
      "FROM \"{cdm}\".drug_exposure de ",
      "JOIN \"{cdm}\".concept c ON c.concept_id = de.drug_concept_id ",
      "WHERE de.drug_concept_id <> 0 GROUP BY c.domain_id"
    ),
    measurement = glue::glue(
      "SELECT 'measurement' AS cdm_table, 'Measurement' AS expected_domain, c.domain_id AS concept_domain, COUNT(*) AS record_count ",
      "FROM \"{cdm}\".measurement m ",
      "JOIN \"{cdm}\".concept c ON c.concept_id = m.measurement_concept_id ",
      "WHERE m.measurement_concept_id <> 0 GROUP BY c.domain_id"
    ),
    observation = glue::glue(
      "SELECT 'observation' AS cdm_table, 'Observation' AS expected_domain, c.domain_id AS concept_domain, COUNT(*) AS record_count ",
      "FROM \"{cdm}\".observation o ",
      "JOIN \"{cdm}\".concept c ON c.concept_id = o.observation_concept_id ",
      "WHERE o.observation_concept_id <> 0 GROUP BY c.domain_id"
    ),
    procedure_occurrence = glue::glue(
      "SELECT 'procedure_occurrence' AS cdm_table, 'Procedure' AS expected_domain, c.domain_id AS concept_domain, COUNT(*) AS record_count ",
      "FROM \"{cdm}\".procedure_occurrence po ",
      "JOIN \"{cdm}\".concept c ON c.concept_id = po.procedure_concept_id ",
      "WHERE po.procedure_concept_id <> 0 GROUP BY c.domain_id"
    )
  )

  df_domain_conf <- tryCatch({
    res <- lapply(domain_conf_queries, function(sql) DBI::dbGetQuery(con, sql))
    raw <- do.call(rbind, res)
    if (!is.null(raw) && nrow(raw) > 0) {
      totals <- stats::aggregate(record_count ~ cdm_table, data = raw, FUN = sum)
      names(totals)[2] <- "total_mapped"
      raw <- merge(raw, totals, by = "cdm_table")
      raw$conforming <- raw$concept_domain == raw$expected_domain
      raw$conformance_pct <- round(100 * raw$record_count / raw$total_mapped, 2)
    }
    raw
  }, error = function(e) {
    data.frame(cdm_table = character(), expected_domain = character(), concept_domain = character(),
               record_count = integer(), total_mapped = integer(), conforming = logical(),
               conformance_pct = numeric(), stringsAsFactors = FALSE)
  })

  # ----- 10. Domain routing log (records moved between tables) -----
  df_routing_log <- tryCatch({
    if (table_exists(con, stg, "domain_routing_log")) {
      DBI::dbGetQuery(con, glue::glue('SELECT * FROM "{stg}".domain_routing_log ORDER BY from_table, to_domain'))
    } else {
      data.frame(from_table = character(), to_domain = character(), record_count = integer(), stringsAsFactors = FALSE)
    }
  }, error = function(e) {
    data.frame(from_table = character(), to_domain = character(), record_count = integer(), stringsAsFactors = FALSE)
  })

  # Return all datasets
  list(
    unmapped_concepts          = df_unmapped,
    top_unmapped_source_values = df_top,
    record_counts              = df_record_counts,
    person_count               = df_person,
    one_to_many                = df_one_to_many,
    many_to_one                = df_many_to_one,
    reject_counts              = df_reject_counts,
    summary                    = df_not_mapped_summary,
    domain_conformance         = df_domain_conf,
    routing_log                = df_routing_log
  )
}
