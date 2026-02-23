# improve_mappings.R
# LLM-powered improvement loop for unmapped OMOP concepts. Queries CDM for
# concept_id = 0 rows, uses GPT-4 with Hecate vocabulary search tools to find
# mappings, writes results to custom_concept_mapping.csv for the next ETL run.

#' Improve unmapped concept mappings using an LLM with vocabulary search tools
#'
#' Queries the DuckDB for records with concept_id = 0, then for each unmapped
#' source value, uses GPT-4 (or compatible model) with Hecate vocabulary search
#' as function-calling tools to find the best OMOP concept mapping.
#'
#' Results are written to a detailed log CSV and, for high-confidence mappings,
#' appended to the custom_concept_mapping.csv that the ETL consumes.
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
#' @param con DBI connection to the DuckDB database (post-ETL, CDM tables must exist).
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
#' @param dry_run If TRUE, show what would be processed without calling LLM.
#' @return Invisible data.frame of all log results.
#' @export
improve_mappings <- function(con,
                             config = NULL,
                             domains = c("condition", "drug", "measurement", "procedure", "observation"),
                             limit = 50L,
                             confidence_threshold = 0.7,
                             custom_mapping_path = NULL,
                             log_path = "mapping_improvement_log.csv",
                             model = NULL,
                             api_key = NULL,
                             hecate = NULL,
                             dry_run = FALSE) {
  config <- config %||% default_etl_config()
  cdm <- config$schemas$cdm %||% "main"

  # Resolve custom_mapping_path
  if (is.null(custom_mapping_path)) {
    custom_mapping_path <- config$custom_mapping_path
    if (is.null(custom_mapping_path) || !nzchar(trimws(custom_mapping_path))) {
      custom_mapping_path <- system.file("extdata", "custom_concept_mapping.csv", package = "ETLDelphi")
    }
  }

  # Load existing log to skip already-processed values
  existing_log <- if (file.exists(log_path)) {
    tryCatch(
      read.csv(log_path, stringsAsFactors = FALSE),
      error = function(e) data.frame(source_value = character(), domain = character(), stringsAsFactors = FALSE)
    )
  } else {
    data.frame(source_value = character(), domain = character(), stringsAsFactors = FALSE)
  }
  already_done <- paste(existing_log$source_value, existing_log$domain, sep = "|||")

  # Load existing custom mappings
  existing_custom <- if (file.exists(custom_mapping_path)) {
    tryCatch(
      read.csv(custom_mapping_path, stringsAsFactors = FALSE),
      error = function(e) data.frame(source_value = character(), domain = character(), concept_id = integer(), stringsAsFactors = FALSE)
    )
  } else {
    data.frame(source_value = character(), domain = character(), concept_id = integer(), stringsAsFactors = FALSE)
  }

  # Query unmapped source values
  unmapped <- get_unmapped_source_values(con, cdm = cdm, domains = domains, limit = limit)

  if (nrow(unmapped) == 0) {
    cli::cli_alert_success("No unmapped source values found.")
    return(invisible(existing_log))
  }

  # Filter out already-processed
  unmapped_key <- paste(unmapped$source_value, unmapped$domain, sep = "|||")
  unmapped <- unmapped[!unmapped_key %in% already_done, , drop = FALSE]

  if (nrow(unmapped) == 0) {
    cli::cli_alert_success("All unmapped source values already processed (see {log_path}).")
    return(invisible(existing_log))
  }

  cli::cli_alert_info("Processing {nrow(unmapped)} unmapped source values across {length(unique(unmapped$domain))} domain(s)")

  if (dry_run) {
    cli::cli_alert_info("Dry run -- would process:")
    for (i in seq_len(nrow(unmapped))) {
      r <- unmapped[i, ]
      cli::cli_bullets(c("*" = "{r$domain}: \"{r$source_value}\" ({r$record_count} records)"))
    }
    return(invisible(unmapped))
  }

  # Initialize clients
  hc <- hecate %||% hecate_client()
  model <- model %||% Sys.getenv("OPENAI_MODEL", "gpt-4o")

  tools <- mapping_tools()
  handlers <- build_tool_handlers(hc)

  # Ensure log file has header
  if (!file.exists(log_path) || file.size(log_path) == 0) {
    writeLines(
      "source_value,domain,record_count,concept_id,concept_name,vocabulary_id,confidence,reasoning,tool_calls,timestamp,model",
      log_path
    )
  }

  new_custom_rows <- list()

  for (i in seq_len(nrow(unmapped))) {
    row <- unmapped[i, ]
    cli::cli_alert_info("[{i}/{nrow(unmapped)}] {row$domain}: \"{row$source_value}\" ({row$record_count} records)")

    result <- tryCatch(
      map_single_value(
        source_value = row$source_value,
        domain = row$domain,
        record_count = row$record_count,
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
          tool_calls = 0L
        )
      }
    )

    # Build log row
    log_row <- data.frame(
      source_value = row$source_value,
      domain = row$domain,
      record_count = row$record_count,
      concept_id = as.integer(result$concept_id %||% NA_integer_),
      concept_name = result$concept_name %||% NA_character_,
      vocabulary_id = result$vocabulary_id %||% NA_character_,
      confidence = as.numeric(result$confidence %||% 0),
      reasoning = result$reasoning %||% "",
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
      new_custom_rows[[length(new_custom_rows) + 1L]] <- data.frame(
        source_value = row$source_value,
        domain = row$domain,
        concept_id = cid,
        stringsAsFactors = FALSE
      )
      cli::cli_alert_success("  -> {result$concept_name} ({cid}) confidence={round(conf, 2)}")
    } else {
      cli::cli_alert_warning("  -> No mapping (confidence={round(conf, 2)})")
    }

    Sys.sleep(0.5)
  }

  # Merge new custom mappings into production CSV
  if (length(new_custom_rows) > 0) {
    new_custom <- do.call(rbind, new_custom_rows)
    merged <- rbind(existing_custom, new_custom)
    # Deduplicate by source_value + domain (keep last = new mapping wins)
    dup_key <- paste(merged$source_value, merged$domain, sep = "|||")
    merged <- merged[!duplicated(dup_key, fromLast = TRUE), , drop = FALSE]
    write.csv(merged, custom_mapping_path, row.names = FALSE)
    cli::cli_alert_success("Added {nrow(new_custom)} new mapping(s) to {custom_mapping_path}")
  }

  # Return full log
  full_log <- tryCatch(
    read.csv(log_path, stringsAsFactors = FALSE),
    error = function(e) existing_log
  )

  invisible(full_log)
}


# --- Internal helpers ---

# Query CDM tables for unmapped source values (concept_id = 0).
# Returns data.frame with columns: source_value, record_count, domain.
get_unmapped_source_values <- function(con, cdm, domains, limit) {
  domain_queries <- list(
    condition = glue::glue(
      'SELECT condition_source_value AS source_value, COUNT(*) AS record_count ',
      'FROM "{cdm}".condition_occurrence WHERE condition_concept_id = 0 ',
      'AND condition_source_value IS NOT NULL AND TRIM(condition_source_value) != \'\' ',
      'GROUP BY condition_source_value ORDER BY record_count DESC LIMIT {limit}'
    ),
    drug = glue::glue(
      'SELECT drug_source_value AS source_value, COUNT(*) AS record_count ',
      'FROM "{cdm}".drug_exposure WHERE drug_concept_id = 0 ',
      'AND drug_source_value IS NOT NULL AND TRIM(drug_source_value) != \'\' ',
      'GROUP BY drug_source_value ORDER BY record_count DESC LIMIT {limit}'
    ),
    measurement = glue::glue(
      'SELECT measurement_source_value AS source_value, COUNT(*) AS record_count ',
      'FROM "{cdm}".measurement WHERE measurement_concept_id = 0 ',
      'AND measurement_source_value IS NOT NULL AND TRIM(measurement_source_value) != \'\' ',
      'GROUP BY measurement_source_value ORDER BY record_count DESC LIMIT {limit}'
    ),
    procedure = glue::glue(
      'SELECT procedure_source_value AS source_value, COUNT(*) AS record_count ',
      'FROM "{cdm}".procedure_occurrence WHERE procedure_concept_id = 0 ',
      'AND procedure_source_value IS NOT NULL AND TRIM(procedure_source_value) != \'\' ',
      'GROUP BY procedure_source_value ORDER BY record_count DESC LIMIT {limit}'
    ),
    observation = glue::glue(
      'SELECT observation_source_value AS source_value, COUNT(*) AS record_count ',
      'FROM "{cdm}".observation WHERE observation_concept_id = 0 ',
      'AND observation_source_value IS NOT NULL AND TRIM(observation_source_value) != \'\' ',
      'GROUP BY observation_source_value ORDER BY record_count DESC LIMIT {limit}'
    )
  )

  tbl_map <- list(
    condition = "condition_occurrence", drug = "drug_exposure",
    measurement = "measurement", procedure = "procedure_occurrence",
    observation = "observation"
  )

  out <- list()
  for (d in domains) {
    if (!d %in% names(domain_queries)) next
    if (!table_exists(con, cdm, tbl_map[[d]])) next
    res <- tryCatch(DBI::dbGetQuery(con, domain_queries[[d]]), error = function(e) NULL)
    if (!is.null(res) && nrow(res) > 0) {
      res$domain <- d
      out[[length(out) + 1L]] <- res
    }
  }

  if (length(out) > 0) do.call(rbind, out) else {
    data.frame(source_value = character(), record_count = integer(),
               domain = character(), stringsAsFactors = FALSE)
  }
}

# Map a single source value using the OpenAI tool-calling loop.
# Returns list with concept_id, concept_name, vocabulary_id, confidence, reasoning, tool_calls.
map_single_value <- function(source_value, domain, record_count,
                             tools, tool_handlers, model, api_key) {
  system_prompt <- build_mapping_system_prompt(domain)

  user_message <- paste0(
    "Map the following source value to an OMOP standard concept:\n",
    "Source value: \"", source_value, "\"\n",
    "Domain: ", domain, "\n",
    "Record count: ", record_count, "\n",
    "\nSearch the vocabulary and return your mapping as JSON."
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
