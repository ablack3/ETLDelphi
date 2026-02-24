# improve_mappings.R
# LLM-powered improvement loop for unmapped OMOP concepts. Queries staging tables
# for rich context (code + name + vocabulary), uses GPT-4 with Hecate/OMOPHub
# vocabulary search tools to find mappings, writes results to custom_concept_mapping.csv
# and custom_ndc_mapping.csv for the next ETL run.

#' Improve unmapped concept mappings using an LLM with vocabulary search tools
#'
#' Queries DuckDB staging tables for records that failed vocabulary mapping
#' (concept_id = 0), then for each unmapped source value, uses GPT-4 (or
#' compatible model) with Hecate semantic search and OMOPHub code lookup tools
#' to find the best OMOP concept mapping.
#'
#' The function provides the LLM with rich context: both the source code AND
#' the human-readable name for each unmapped value (e.g., both the NDC code
#' and the drug name), enabling more accurate mapping.
#'
#' Results are written to a detailed log CSV and, for high-confidence mappings,
#' appended to the custom_concept_mapping.csv and custom_ndc_mapping.csv files
#' that the ETL consumes.
#'
#' @section Pipeline integration:
#' \preformatted{
#' # 1. Run ETL
#' run_etl(con)
#'
#' # 2. Improve unmapped concepts
#' improve_mappings(con, limit = 50, confidence_threshold = 0.7)
#'
#' # 3. Re-run ETL from vocab step to pick up new mappings
#' run_etl(con, from_step = "30_vocab")
#' }
#'
#' @param con DBI connection to the DuckDB database (post-ETL, staging + CDM tables must exist).
#' @param config ETL config list (for schema names). Default: \code{default_etl_config()}.
#' @param domains Character vector of domains to improve. Default: all five.
#' @param limit Max unmapped source values to process per domain (ordered by record count).
#' @param confidence_threshold Minimum confidence to write to production CSV. Default: 0.7.
#' @param custom_mapping_path Path to production custom_concept_mapping.csv.
#'   Default: from config or package default.
#' @param log_path Path to detailed log CSV. Default: \code{"mapping_improvement_log.csv"}.
#' @param model OpenAI model name. Default: env var OPENAI_MODEL or \code{"gpt-4o"}.
#' @param api_key OpenAI API key. Default: env var OPENAI_API_KEY.
#' @param hecate Hecate client object. Default: created from env vars.
#' @param omophub OMOPHub client object. Default: created from env vars. Used for NDC lookups.
#' @param custom_ndc_mapping_path Path to custom NDC mapping CSV. Default: from config or package default.
#' @param force_retry Controls retry behaviour for already-processed values.
#'   \itemize{
#'     \item \code{FALSE} (default): skip all values already in the log.
#'     \item \code{"failed"}: retry values where the log has concept_id = 0/NA or confidence below threshold.
#'     \item \code{TRUE}: ignore the log entirely — reprocess everything.
#'   }
#' @param dry_run If TRUE, show what would be processed without calling LLM.
#' @return Invisible data.frame of all log results.
#' @export
improve_mappings <- function(con,
                             config = NULL,
                             domains = c("condition", "drug", "measurement", "procedure", "observation"),
                             limit = 50L,
                             confidence_threshold = 0.7,
                             custom_mapping_path = NULL,
                             custom_ndc_mapping_path = NULL,
                             log_path = "mapping_improvement_log.csv",
                             model = NULL,
                             api_key = NULL,
                             hecate = NULL,
                             omophub = NULL,
                             force_retry = FALSE,
                             dry_run = FALSE) {
  config <- config %||% default_etl_config()
  stg <- config$schemas$stg %||% "stg"

  # Resolve custom_mapping_path
  if (is.null(custom_mapping_path)) {
    custom_mapping_path <- config$custom_mapping_path
    if (is.null(custom_mapping_path) || !nzchar(trimws(custom_mapping_path))) {
      custom_mapping_path <- system.file("extdata", "custom_concept_mapping.csv", package = "ETLDelphi")
    }
  }

  # Resolve custom_ndc_mapping_path
  if (is.null(custom_ndc_mapping_path)) {
    custom_ndc_mapping_path <- config$custom_ndc_mapping_path
    if (is.null(custom_ndc_mapping_path) || !nzchar(trimws(custom_ndc_mapping_path))) {
      custom_ndc_mapping_path <- system.file("extdata", "custom_ndc_mapping.csv", package = "ETLDelphi")
    }
  }

  # --- Build already-done set and populate temp table ---
  existing_log <- load_existing_log(log_path)
  populate_already_done_table(con, existing_log, force_retry, confidence_threshold)

  # Load existing custom mappings
  existing_custom <- if (file.exists(custom_mapping_path)) {
    tryCatch(
      read.csv(custom_mapping_path, stringsAsFactors = FALSE),
      error = function(e) data.frame(source_value = character(), domain = character(), concept_id = integer(), stringsAsFactors = FALSE)
    )
  } else {
    data.frame(source_value = character(), domain = character(), concept_id = integer(), stringsAsFactors = FALSE)
  }

  # Query unmapped source values from staging (with rich context)
  unmapped <- get_unmapped_source_values(con, stg = stg, domains = domains, limit = limit)

  if (nrow(unmapped) == 0) {
    cli::cli_alert_success("No unmapped source values found across requested domain(s).")
    cleanup_already_done_table(con)
    return(invisible(existing_log))
  }

  cli::cli_alert_info("Processing {nrow(unmapped)} unmapped source values across {length(unique(unmapped$domain))} domain(s)")

  if (dry_run) {
    cli::cli_alert_info("Dry run -- would process:")
    for (i in seq_len(nrow(unmapped))) {
      r <- unmapped[i, ]
      name_info <- if (!is.na(r$source_name) && nzchar(r$source_name)) paste0(" [", r$source_name, "]") else ""
      code_info <- if (!is.na(r$source_code) && nzchar(r$source_code)) paste0(" (code: ", r$source_code, ")") else ""
      cli::cli_bullets(c("*" = "{r$domain}: \"{r$source_value}\"{name_info}{code_info} ({r$record_count} records)"))
    }
    cleanup_already_done_table(con)
    return(invisible(unmapped))
  }

  # Initialize clients
  hc <- hecate %||% hecate_client()
  model <- model %||% Sys.getenv("OPENAI_MODEL", "gpt-4o")

  # OMOPHub client for NDC lookups (only used for drug domain)
  has_omophub <- nzchar(Sys.getenv("OMOPHUB_API_KEY", ""))
  oh <- if ("drug" %in% domains && (has_omophub || !is.null(omophub))) {
    omophub %||% omophub_client()
  } else {
    NULL
  }

  if ("drug" %in% domains && is.null(oh)) {
    cli::cli_alert_warning("OMOPHUB_API_KEY not set. NDC lookup tools will not be available for drug mapping.")
  }

  # Load existing NDC mappings
  existing_ndc <- if (file.exists(custom_ndc_mapping_path)) {
    tryCatch(
      read.csv(custom_ndc_mapping_path, stringsAsFactors = FALSE),
      error = function(e) data.frame(drug_ndc_normalized = character(), drug_concept_id = integer(), stringsAsFactors = FALSE)
    )
  } else {
    data.frame(drug_ndc_normalized = character(), drug_concept_id = integer(), stringsAsFactors = FALSE)
  }

  # Ensure log file has header (with new columns)
  if (!file.exists(log_path) || file.size(log_path) == 0) {
    writeLines(
      "source_value,domain,record_count,source_code,source_name,mapping_key,concept_id,concept_name,vocabulary_id,confidence,reasoning,source_is_ndc,ndc_normalized,tool_calls,timestamp,model",
      log_path
    )
  }

  new_custom_rows <- list()
  new_ndc_rows <- list()

  for (i in seq_len(nrow(unmapped))) {
    row <- unmapped[i, ]
    name_info <- if (!is.na(row$source_name) && nzchar(row$source_name)) paste0(" [", row$source_name, "]") else ""
    cli::cli_alert_info("[{i}/{nrow(unmapped)}] {row$domain}: \"{row$source_value}\"{name_info} ({row$record_count} records)")

    # Build domain-specific tools and handlers
    domain <- row$domain
    tools <- mapping_tools(domain = domain)
    handlers <- build_tool_handlers(hc, oh = oh, domain = domain)

    result <- tryCatch(
      map_single_value(
        source_value = row$source_value,
        domain = domain,
        record_count = row$record_count,
        source_code = row$source_code,
        source_name = row$source_name,
        source_vocab = row$source_vocab,
        tools = tools,
        tool_handlers = handlers,
        model = model,
        api_key = api_key
      ),
      error = function(e) {
        cli::cli_warn("  Error: {conditionMessage(e)}")
        list(
          concept_id = NA_integer_, concept_name = NA_character_,
          vocabulary_id = NA_character_, confidence = 0,
          reasoning = paste("Error:", conditionMessage(e)),
          source_is_ndc = FALSE, ndc_normalized = NA_character_,
          tool_calls = 0L
        )
      }
    )

    # Build log row (with new context columns)
    log_row <- data.frame(
      source_value = row$source_value,
      domain = domain,
      record_count = row$record_count,
      source_code = row$source_code %||% NA_character_,
      source_name = row$source_name %||% NA_character_,
      mapping_key = row$mapping_key %||% row$source_value,
      concept_id = as.integer(result$concept_id %||% NA_integer_),
      concept_name = result$concept_name %||% NA_character_,
      vocabulary_id = result$vocabulary_id %||% NA_character_,
      confidence = as.numeric(result$confidence %||% 0),
      reasoning = result$reasoning %||% "",
      source_is_ndc = as.logical(result$source_is_ndc %||% FALSE),
      ndc_normalized = result$ndc_normalized %||% NA_character_,
      tool_calls = as.integer(result$tool_calls %||% 0L),
      timestamp = format(Sys.time(), "%Y-%m-%dT%H:%M:%S"),
      model = model,
      stringsAsFactors = FALSE
    )

    # Append to log incrementally
    write.table(log_row, log_path, append = TRUE, sep = ",",
                row.names = FALSE, col.names = FALSE, quote = TRUE)

    # If above threshold, queue for custom mapping
    conf <- as.numeric(result$confidence %||% 0)
    cid <- as.integer(result$concept_id %||% NA_integer_)
    if (!is.na(cid) && cid > 0 && !is.na(conf) && conf >= confidence_threshold) {
      # Use mapping_key for custom_concept_mapping.csv
      # For drugs, mapping_key = drug_name (matches CDM load JOIN on drug_name)
      # For others, mapping_key = source_value (matches CDM load JOIN on COALESCE(code, name))
      mk <- row$mapping_key
      if (is.null(mk) || is.na(mk) || !nzchar(mk)) mk <- row$source_value

      new_custom_rows[[length(new_custom_rows) + 1L]] <- data.frame(
        source_value = mk,
        domain = domain,
        concept_id = cid,
        stringsAsFactors = FALSE
      )

      # If this is an NDC drug mapping, also add to custom_ndc_mapping.csv
      is_ndc <- isTRUE(result$source_is_ndc) || (domain == "drug" && !is.na(row$source_code) && nzchar(row$source_code) && is_ndc_like(row$source_code))
      if (is_ndc && domain == "drug") {
        # Use the LLM's normalized NDC, or fall back to source_code (the ndc_normalized from staging)
        ndc_norm <- result$ndc_normalized
        if (is.null(ndc_norm) || is.na(ndc_norm) || !nzchar(ndc_norm)) {
          ndc_norm <- row$source_code
        }
        new_ndc_rows[[length(new_ndc_rows) + 1L]] <- data.frame(
          drug_ndc_normalized = ndc_norm,
          drug_concept_id = cid,
          stringsAsFactors = FALSE
        )
        cli::cli_alert_success("  -> {result$concept_name} ({cid}) [NDC: {ndc_norm}] confidence={round(conf, 2)}")
      } else {
        cli::cli_alert_success("  -> {result$concept_name} ({cid}) confidence={round(conf, 2)}")
      }
    } else {
      cli::cli_alert_warning("  -> No mapping (confidence={round(conf, 2)})")
    }

    Sys.sleep(0.5)
  }

  # Merge new custom mappings into production CSV
  if (length(new_custom_rows) > 0) {
    new_custom <- do.call(rbind, new_custom_rows)
    merged <- rbind(existing_custom, new_custom)
    dup_key <- paste(merged$source_value, merged$domain, sep = "|||")
    merged <- merged[!duplicated(dup_key, fromLast = TRUE), , drop = FALSE]
    write.csv(merged, custom_mapping_path, row.names = FALSE)
    cli::cli_alert_success("Added {nrow(new_custom)} new mapping(s) to {custom_mapping_path}")
  }

  # Merge new NDC mappings into custom_ndc_mapping.csv
  if (length(new_ndc_rows) > 0) {
    new_ndc <- do.call(rbind, new_ndc_rows)
    merged_ndc <- rbind(existing_ndc, new_ndc)
    merged_ndc <- merged_ndc[!duplicated(merged_ndc$drug_ndc_normalized, fromLast = TRUE), , drop = FALSE]
    write.csv(merged_ndc, custom_ndc_mapping_path, row.names = FALSE)
    cli::cli_alert_success("Added {nrow(new_ndc)} NDC mapping(s) to {custom_ndc_mapping_path}")
  }

  # Cleanup temp table
  cleanup_already_done_table(con)

  # Return full log
  full_log <- tryCatch(
    read.csv(log_path, stringsAsFactors = FALSE),
    error = function(e) existing_log
  )

  invisible(full_log)
}


# --- Internal helpers ---

# Load existing log file, returning empty data.frame if missing/corrupt.
load_existing_log <- function(log_path) {
  if (file.exists(log_path) && file.size(log_path) > 0) {
    tryCatch(
      read.csv(log_path, stringsAsFactors = FALSE),
      error = function(e) empty_log_df()
    )
  } else {
    empty_log_df()
  }
}

empty_log_df <- function() {
  data.frame(
    source_value = character(), domain = character(),
    concept_id = integer(), confidence = numeric(),
    stringsAsFactors = FALSE
  )
}

# Create a DuckDB temp table of already-processed (source_value, domain) pairs.
# The staging queries use NOT EXISTS against this table so the SQL LIMIT applies
# only to truly new values.
populate_already_done_table <- function(con, existing_log, force_retry, confidence_threshold) {
  tryCatch(DBI::dbExecute(con, "DROP TABLE IF EXISTS _improve_already_done"), error = function(e) NULL)
  DBI::dbExecute(con, "CREATE TEMP TABLE _improve_already_done (source_value VARCHAR, domain VARCHAR)")

  if (isTRUE(force_retry)) {
    # Skip nothing — leave the table empty
    return(invisible(NULL))
  }

  if (nrow(existing_log) == 0) return(invisible(NULL))

  # Determine which log entries count as "done"
  if (identical(force_retry, "failed")) {
    # Only mark successful entries as done (retry the failures)
    keep <- !is.na(existing_log$concept_id) &
      existing_log$concept_id > 0 &
      !is.na(existing_log$confidence) &
      existing_log$confidence >= confidence_threshold
    done_log <- existing_log[keep, , drop = FALSE]
  } else {
    # Default: all logged entries are done
    done_log <- existing_log
  }

  if (nrow(done_log) == 0) return(invisible(NULL))

  # De-duplicate (same source_value + domain may appear multiple times in log)
  done_pairs <- unique(done_log[, c("source_value", "domain"), drop = FALSE])

  # Insert into temp table in batches to avoid huge SQL
  batch_size <- 500L
  for (start in seq(1L, nrow(done_pairs), by = batch_size)) {
    end <- min(start + batch_size - 1L, nrow(done_pairs))
    batch <- done_pairs[start:end, , drop = FALSE]

    values <- vapply(seq_len(nrow(batch)), function(i) {
      sv <- gsub("'", "''", batch$source_value[i])
      dm <- gsub("'", "''", batch$domain[i])
      sprintf("('%s', '%s')", sv, dm)
    }, character(1))

    sql <- paste0("INSERT INTO _improve_already_done VALUES ", paste(values, collapse = ", "))
    tryCatch(DBI::dbExecute(con, sql), error = function(e) {
      cli::cli_warn("Failed to insert batch into _improve_already_done: {conditionMessage(e)}")
    })
  }

  invisible(NULL)
}

cleanup_already_done_table <- function(con) {
  tryCatch(DBI::dbExecute(con, "DROP TABLE IF EXISTS _improve_already_done"), error = function(e) NULL)
}


# Query staging tables for unmapped source values with rich context.
# Returns data.frame: source_value, record_count, domain, source_code, source_name,
#                     source_vocab, mapping_key
# The already-done temp table (_improve_already_done) is used to exclude previously
# processed values BEFORE the LIMIT is applied.
get_unmapped_source_values <- function(con, stg, domains, limit) {
  # Staging table requirements per domain
  stg_tables <- list(
    drug = c("medication_orders", "map_drug_order"),
    condition = c("problem", "map_condition"),
    measurement = c("lab_results", "map_loinc_measurement"),
    procedure = c("therapy_orders", "map_therapy"),
    observation = c("allergy")
  )

  out <- list()

  for (d in domains) {
    required <- stg_tables[[d]]
    if (is.null(required)) next

    # Check all required staging tables exist
    missing <- vapply(required, function(t) !table_exists(con, stg, t), logical(1))
    if (any(missing)) {
      cli::cli_alert_warning("Skipping {d}: staging table(s) {paste(required[missing], collapse=', ')} not found in {stg} schema.")
      next
    }

    sql <- build_unmapped_query(d, stg, limit)
    if (is.null(sql)) next

    res <- tryCatch(DBI::dbGetQuery(con, sql), error = function(e) {
      cli::cli_warn("Error querying unmapped {d}: {conditionMessage(e)}")
      NULL
    })

    if (!is.null(res) && nrow(res) > 0) {
      res$domain <- d
      # Ensure all expected columns exist
      for (col in c("source_code", "source_name", "source_vocab", "mapping_key")) {
        if (!col %in% names(res)) res[[col]] <- NA_character_
      }
      out[[length(out) + 1L]] <- res
    }
  }

  if (length(out) > 0) {
    result <- do.call(rbind, out)
    # Standardize column order
    cols <- c("source_value", "record_count", "domain", "source_code", "source_name", "source_vocab", "mapping_key")
    result[, intersect(cols, names(result)), drop = FALSE]
  } else {
    data.frame(
      source_value = character(), record_count = integer(),
      domain = character(), source_code = character(),
      source_name = character(), source_vocab = character(),
      mapping_key = character(), stringsAsFactors = FALSE
    )
  }
}


# Build domain-specific SQL to find unmapped values from staging tables.
# Each query joins staging data to its mapping table and custom_concept_mapping,
# checks for COALESCE(...) = 0, excludes already-done values, and returns
# rich context columns.
build_unmapped_query <- function(domain, stg, limit) {
  # Common NOT EXISTS clause against the already-done temp table.
  # The {sv_expr} placeholder is the expression that produces source_value.
  not_exists <- function(sv_expr, domain_str) {
    glue::glue(
      'AND NOT EXISTS (',
      '  SELECT 1 FROM _improve_already_done ad ',
      '  WHERE ad.source_value = {sv_expr} AND ad.domain = \'{domain_str}\'',
      ')'
    )
  }

  switch(domain,
    drug = {
      # Drug: query map_drug_order joined to medication_orders for counts.
      # mapping_key = drug_name (matches CDM load JOIN on drug_name, NOT on drug_source_value).
      # source_code = drug_ndc_normalized, source_name = drug_name.
      sv <- "SUBSTR(COALESCE(NULLIF(TRIM(d.drug_ndc_normalized), ''), d.drug_name), 1, 50)"
      ne <- not_exists(sv, "drug")
      glue::glue(
        'SELECT ',
        '  {sv} AS source_value, ',
        '  COUNT(mo.order_id) AS record_count, ',
        '  d.drug_ndc_normalized AS source_code, ',
        '  d.drug_name AS source_name, ',
        '  CASE WHEN d.drug_ndc_normalized IS NOT NULL AND TRIM(d.drug_ndc_normalized) != \'\' THEN \'NDC\' ELSE NULL END AS source_vocab, ',
        '  TRIM(SUBSTR(d.drug_name, 1, 50)) AS mapping_key ',
        'FROM "{stg}".map_drug_order d ',
        'JOIN "{stg}".medication_orders mo ',
        '  ON (d.drug_ndc_normalized = mo.drug_ndc_normalized ',
        '      OR (d.drug_ndc_normalized IS NULL AND mo.drug_ndc_normalized IS NULL)) ',
        '  AND d.drug_name = mo.drug_name ',
        'LEFT JOIN "{stg}".custom_concept_mapping cust ',
        '  ON cust.source_value = TRIM(SUBSTR(d.drug_name, 1, 50)) AND cust.domain = \'drug\' ',
        'LEFT JOIN "{stg}".custom_ndc_mapping cndc ',
        '  ON cndc.drug_ndc_normalized = d.drug_ndc_normalized ',
        'WHERE COALESCE(d.drug_concept_id, cust.concept_id, cndc.drug_concept_id, 0) = 0 ',
        '  AND COALESCE(d.drug_ndc_normalized, d.drug_name) IS NOT NULL ',
        '  {ne} ',
        'GROUP BY d.drug_name, d.drug_ndc_normalized ',
        'ORDER BY record_count DESC ',
        'LIMIT {limit}'
      )
    },
    condition = {
      # Condition: query problem joined to map_condition.
      # source_code = problem_code, source_name = problem_description.
      sv <- "SUBSTR(COALESCE(p.problem_code, p.problem_description), 1, 50)"
      ne <- not_exists(sv, "condition")
      glue::glue(
        'SELECT ',
        '  {sv} AS source_value, ',
        '  COUNT(*) AS record_count, ',
        '  p.problem_code AS source_code, ',
        '  p.problem_description AS source_name, ',
        '  p.problem_type AS source_vocab, ',
        '  {sv} AS mapping_key ',
        'FROM "{stg}".problem p ',
        'LEFT JOIN "{stg}".map_condition mc ON mc.problem_code = p.problem_code ',
        'LEFT JOIN "{stg}".custom_concept_mapping cust ',
        '  ON cust.source_value = TRIM({sv}) AND cust.domain = \'condition\' ',
        'WHERE COALESCE(mc.condition_concept_id, cust.concept_id, 0) = 0 ',
        '  AND COALESCE(p.problem_code, p.problem_description) IS NOT NULL ',
        '  AND TRIM(COALESCE(p.problem_code, p.problem_description)) != \'\' ',
        '  {ne} ',
        'GROUP BY p.problem_code, p.problem_description, p.problem_type ',
        'ORDER BY record_count DESC ',
        'LIMIT {limit}'
      )
    },
    measurement = {
      # Measurement: query lab_results joined to map_loinc_measurement.
      # source_code = test_loinc, source_name = test_name.
      sv <- "SUBSTR(COALESCE(lr.test_loinc, lr.test_name), 1, 50)"
      ne <- not_exists(sv, "measurement")
      glue::glue(
        'SELECT ',
        '  {sv} AS source_value, ',
        '  COUNT(*) AS record_count, ',
        '  lr.test_loinc AS source_code, ',
        '  lr.test_name AS source_name, ',
        '  \'LOINC\' AS source_vocab, ',
        '  {sv} AS mapping_key ',
        'FROM "{stg}".lab_results lr ',
        'LEFT JOIN "{stg}".map_loinc_measurement lm ON lm.loinc_code = lr.test_loinc ',
        'LEFT JOIN "{stg}".custom_concept_mapping cust ',
        '  ON cust.source_value = TRIM({sv}) AND cust.domain = \'measurement\' ',
        'WHERE COALESCE(lm.measurement_concept_id, cust.concept_id, 0) = 0 ',
        '  AND COALESCE(lr.test_loinc, lr.test_name) IS NOT NULL ',
        '  AND TRIM(COALESCE(lr.test_loinc, lr.test_name)) != \'\' ',
        '  {ne} ',
        'GROUP BY lr.test_loinc, lr.test_name ',
        'ORDER BY record_count DESC ',
        'LIMIT {limit}'
      )
    },
    procedure = {
      # Procedure: query therapy_orders + therapy_actions joined to map_therapy.
      # source_code = code, source_name = name, source_vocab = vocabulary.
      sv <- "SUBSTR(COALESCE(t.code, t.name), 1, 50)"
      ne <- not_exists(sv, "procedure")
      glue::glue(
        'WITH therapy_all AS ( ',
        '  SELECT code, name, target_area, vocabulary, encounter_id FROM "{stg}".therapy_orders ',
        '  UNION ALL ',
        '  SELECT code, name, target_area, vocabulary, encounter_id FROM "{stg}".therapy_actions ',
        ') ',
        'SELECT ',
        '  {sv} AS source_value, ',
        '  COUNT(*) AS record_count, ',
        '  t.code AS source_code, ',
        '  t.name AS source_name, ',
        '  t.vocabulary AS source_vocab, ',
        '  {sv} AS mapping_key ',
        'FROM therapy_all t ',
        'LEFT JOIN "{stg}".map_therapy mt ',
        '  ON mt.code = t.code AND (mt.vocabulary = t.vocabulary OR (mt.vocabulary IS NULL AND t.vocabulary IS NULL)) ',
        'LEFT JOIN "{stg}".custom_concept_mapping cust ',
        '  ON cust.source_value = TRIM({sv}) AND cust.domain = \'procedure\' ',
        'WHERE COALESCE(mt.procedure_concept_id, cust.concept_id, 0) = 0 ',
        '  AND COALESCE(t.code, t.name) IS NOT NULL ',
        '  AND TRIM(COALESCE(t.code, t.name)) != \'\' ',
        '  {ne} ',
        'GROUP BY t.code, t.name, t.vocabulary ',
        'ORDER BY record_count DESC ',
        'LIMIT {limit}'
      )
    },
    observation = {
      # Observation: query allergy joined to map_allergy_code and map_allergy.
      # source_name = allergen, source_code = drug_code, source_vocab = drug_vocab.
      sv <- "SUBSTR(COALESCE(a.allergen, a.drug_code), 1, 50)"
      ne <- not_exists(sv, "observation")

      # Check if map_allergy_code and map_allergy exist (may not if allergy mapping wasn't run)
      has_mac <- table_exists(con, stg, "map_allergy_code")
      has_ma <- table_exists(con, stg, "map_allergy")

      mac_join <- if (has_mac) {
        glue::glue(
          'LEFT JOIN "{stg}".map_allergy_code mac ',
          '  ON mac.drug_code = TRIM(a.drug_code) AND mac.drug_vocab = TRIM(UPPER(a.drug_vocab)) ',
          '  AND a.drug_code IS NOT NULL AND TRIM(a.drug_code) != \'\' ',
          '  AND a.drug_vocab IS NOT NULL AND TRIM(UPPER(a.drug_vocab)) IN (\'CVX\', \'NDC\') '
        )
      } else ""

      ma_join <- if (has_ma) {
        glue::glue(
          'LEFT JOIN "{stg}".map_allergy ma ',
          '  ON ma.source_value = TRIM(COALESCE(a.allergen, a.drug_code)) '
        )
      } else ""

      # Build COALESCE for concept check based on available mapping tables
      concept_parts <- c()
      if (has_mac) concept_parts <- c(concept_parts, "mac.observation_concept_id")
      if (has_ma) concept_parts <- c(concept_parts, "ma.observation_concept_id")
      concept_parts <- c(concept_parts, "cust.concept_id", "0")
      coalesce_expr <- paste0("COALESCE(", paste(concept_parts, collapse = ", "), ")")

      glue::glue(
        'SELECT ',
        '  {sv} AS source_value, ',
        '  COUNT(*) AS record_count, ',
        '  a.drug_code AS source_code, ',
        '  a.allergen AS source_name, ',
        '  a.drug_vocab AS source_vocab, ',
        '  {sv} AS mapping_key ',
        'FROM "{stg}".allergy a ',
        '{mac_join} ',
        '{ma_join} ',
        'LEFT JOIN "{stg}".custom_concept_mapping cust ',
        '  ON cust.source_value = TRIM({sv}) AND cust.domain = \'observation\' ',
        'WHERE {coalesce_expr} = 0 ',
        '  AND COALESCE(a.allergen, a.drug_code) IS NOT NULL ',
        '  AND TRIM(COALESCE(a.allergen, a.drug_code)) != \'\' ',
        '  AND a.onset_date IS NOT NULL ',
        '  {ne} ',
        'GROUP BY a.allergen, a.drug_code, a.drug_vocab ',
        'ORDER BY record_count DESC ',
        'LIMIT {limit}'
      )
    },
    {
      cli::cli_warn("Unknown domain: {domain}")
      NULL
    }
  )
}


# Build a rich, domain-aware user message for the LLM.
build_mapping_user_message <- function(source_value, domain, record_count,
                                       source_code = NA, source_name = NA, source_vocab = NA) {
  has_code <- !is.na(source_code) && nzchar(source_code)
  has_name <- !is.na(source_name) && nzchar(source_name)
  has_vocab <- !is.na(source_vocab) && nzchar(source_vocab)

  parts <- c("Map the following source value to an OMOP standard concept:\n")

  if (domain == "drug") {
    if (has_name) parts <- c(parts, paste0("Drug Name: \"", source_name, "\""))
    if (has_code) parts <- c(parts, paste0("Drug NDC (normalized): \"", source_code, "\""))
    if (has_name && has_code) {
      parts <- c(parts, "Use BOTH the drug name and NDC code to find and cross-check the correct mapping.")
    } else if (has_code) {
      parts <- c(parts, "This appears to be an NDC code. Try lookup_ndc first, then search by any name you can infer.")
    } else if (has_name) {
      parts <- c(parts, "No NDC code available. Search by drug name in RxNorm.")
    }
  } else if (domain == "condition") {
    if (has_code) parts <- c(parts, paste0("Problem Code: \"", source_code, "\""))
    if (has_name) parts <- c(parts, paste0("Problem Description: \"", source_name, "\""))
    if (has_vocab) parts <- c(parts, paste0("Problem Type: \"", source_vocab, "\""))
    if (has_code && has_name) {
      parts <- c(parts, "Use the problem code to search (it may be ICD-10-CM, ICD-9-CM, or SNOMED). Use the description to verify your match.")
    } else if (has_name && !has_code) {
      parts <- c(parts, "No code available. Use the text description to search SNOMED for the best standard concept. Use your medical knowledge to infer the clinical concept.")
    }
  } else if (domain == "measurement") {
    if (has_code) parts <- c(parts, paste0("Test LOINC Code: \"", source_code, "\""))
    if (has_name) parts <- c(parts, paste0("Test Name: \"", source_name, "\""))
    if (has_code && has_name) {
      parts <- c(parts, "Search by LOINC code first, then verify against the test name.")
    } else if (has_name && !has_code) {
      parts <- c(parts, "No LOINC code available. Search by test name in LOINC vocabulary. Use your medical knowledge to identify the correct lab test concept.")
    }
  } else if (domain == "procedure") {
    if (has_code) parts <- c(parts, paste0("Procedure Code: \"", source_code, "\""))
    if (has_name) parts <- c(parts, paste0("Procedure Name: \"", source_name, "\""))
    if (has_vocab) parts <- c(parts, paste0("Source Vocabulary: \"", source_vocab, "\""))
    if (has_code && has_vocab) {
      parts <- c(parts, paste0("Search by code in the ", source_vocab, " vocabulary first. Use the name to verify."))
    } else if (has_name) {
      parts <- c(parts, "Search by procedure name in SNOMED, CPT4, or HCPCS.")
    }
  } else if (domain == "observation") {
    if (has_name) parts <- c(parts, paste0("Allergen: \"", source_name, "\""))
    if (has_code) parts <- c(parts, paste0("Drug Code: \"", source_code, "\""))
    if (has_vocab) parts <- c(parts, paste0("Code Vocabulary: \"", source_vocab, "\""))
    if (has_name) {
      parts <- c(parts, "Search SNOMED with domain_id='Observation' for this allergen. Use your clinical knowledge to map allergy descriptions to standard concepts.")
    }
  }

  parts <- c(parts,
    paste0("Domain: ", domain),
    paste0("Record count: ", record_count),
    "",
    "Search the vocabulary using the available tools and return your mapping as JSON."
  )

  paste(parts, collapse = "\n")
}


# Map a single source value using the OpenAI tool-calling loop.
# Accepts rich context from staging: source_code, source_name, source_vocab.
# Returns list with concept_id, concept_name, vocabulary_id, confidence, reasoning, tool_calls.
map_single_value <- function(source_value, domain, record_count,
                             source_code = NA, source_name = NA, source_vocab = NA,
                             tools, tool_handlers, model, api_key) {
  system_prompt <- build_mapping_system_prompt(domain)

  user_message <- build_mapping_user_message(
    source_value = source_value,
    domain = domain,
    record_count = record_count,
    source_code = source_code,
    source_name = source_name,
    source_vocab = source_vocab
  )

  messages <- list(
    list(role = "system", content = system_prompt),
    list(role = "user", content = user_message)
  )

  result <- openai_tool_loop(
    messages = messages,
    tools = tools,
    tool_handlers = tool_handlers,
    max_iterations = 10L,
    model = model,
    api_key = api_key
  )

  # Parse the final JSON response
  parsed <- tryCatch(
    jsonlite::fromJSON(result$final_message, simplifyVector = FALSE),
    error = function(e) {
      # Try to extract JSON from possible markdown fence
      m <- regmatches(result$final_message, regexpr("\\{[^{}]*(?:\\{[^{}]*\\}[^{}]*)*\\}", result$final_message, perl = TRUE))
      if (length(m) > 0 && nchar(m[1]) > 2) {
        tryCatch(
          jsonlite::fromJSON(m[1], simplifyVector = FALSE),
          error = function(e2) {
            list(concept_id = NA_integer_, confidence = 0,
                 reasoning = "Failed to parse LLM response")
          }
        )
      } else {
        list(concept_id = NA_integer_, confidence = 0,
             reasoning = "Failed to parse LLM response")
      }
    }
  )

  parsed$tool_calls <- result$tool_calls_made
  parsed
}
