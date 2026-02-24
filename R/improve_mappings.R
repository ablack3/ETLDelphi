# improve_mappings.R
# LLM-powered improvement loop for unmapped OMOP concepts. Queries staging tables
# for rich context (code + name + vocabulary), uses GPT-4 with Hecate/OMOPHub
# vocabulary search tools to find mappings, writes results to custom_concept_mapping.csv
# and custom_ndc_mapping.csv for the next ETL run.

#' Improve unmapped concept mappings using an LLM with vocabulary search tools
#'
#' Queries DuckDB staging tables for records that failed vocabulary mapping
#' (concept_id = 0), then for each unmapped source value, uses an LLM (Claude
#' or GPT-4) with Hecate semantic search and OMOPHub code lookup tools
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
#' @param provider LLM provider: \code{"anthropic"} or \code{"openai"}. Default:
#'   auto-detected from API keys (prefers Anthropic if both are set).
#' @param model Model name. Default: \code{ANTHROPIC_MODEL} / \code{"claude-sonnet-4-20250514"}
#'   for Anthropic, \code{OPENAI_MODEL} / \code{"gpt-4o"} for OpenAI.
#' @param api_key API key. Default: from \code{ANTHROPIC_API_KEY} or \code{OPENAI_API_KEY}
#'   env var based on provider.
#' @param hecate Hecate client object. Default: created from env vars.
#' @param omophub OMOPHub client object. Default: created from env vars. Used for NDC lookups.
#' @param custom_ndc_mapping_path Path to custom NDC mapping CSV. Default: from config or package default.
#' @param force_retry Controls retry behaviour for already-processed values.
#'   \itemize{
#'     \item \code{FALSE} (default): skip successfully mapped values (concept_id > 0
#'       AND confidence >= threshold). Failed mappings are automatically retried.
#'     \item \code{"all"}: skip ALL values already in the log, including failures.
#'     \item \code{TRUE}: ignore the log entirely — reprocess everything.
#'   }
#' @param dry_run If TRUE, show what would be processed without calling LLM.
#' @return Invisible data.frame of all log results.
#' @export
improve_mappings <- function(con,
                             config = NULL,
                             domains = c("condition", "drug", "measurement", "measurement_value", "procedure", "observation"),
                             limit = 50L,
                             confidence_threshold = 0.7,
                             custom_mapping_path = NULL,
                             custom_ndc_mapping_path = NULL,
                             log_path = "mapping_improvement_log.csv",
                             provider = NULL,
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

  # Resolve LLM provider
  provider <- provider %||% detect_llm_provider()
  provider <- match.arg(provider, c("anthropic", "openai"))

  if (provider == "anthropic") {
    model <- model %||% Sys.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")
    api_key <- api_key %||% Sys.getenv("ANTHROPIC_API_KEY")
  } else {
    model <- model %||% Sys.getenv("OPENAI_MODEL", "gpt-4o")
    api_key <- api_key %||% Sys.getenv("OPENAI_API_KEY")
  }
  cli::cli_alert_info("Using LLM provider: {provider} ({model})")

  # Initialize clients
  hc <- hecate %||% hecate_client()

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

  # Ensure log file has correct 16-column header; repair if outdated
  if (!file.exists(log_path) || file.size(log_path) == 0) {
    writeLines(LOG_HEADER_LINE, log_path)
  } else {
    current_header <- readLines(log_path, n = 1, warn = FALSE)
    if (current_header != LOG_HEADER_LINE) {
      cli::cli_alert_warning("Log file header mismatch \u2014 repairing column alignment...")
      repair_mapping_log(log_path)
    }
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

    # Track whether we hit a rate limit (used to break the loop)
    rate_limited <- FALSE

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
        provider = provider,
        model = model,
        api_key = api_key
      ),
      rate_limit_error = function(e) {
        rate_limited <<- TRUE
        cli::cli_alert_danger("API rate limit reached. Stopping — run again later to continue from where you left off.")
        cli::cli_alert_info("Processed {i - 1L} of {nrow(unmapped)} values before hitting the limit.")
        NULL
      },
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

    # Stop the loop on rate limit — don't log this item so it retries next run
    if (rate_limited) break

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

    # Append to log incrementally (explicit column ordering matches header)
    log_row <- log_row[, LOG_HEADER_COLS, drop = FALSE]
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
# Automatically repairs column alignment if the header doesn't match the current schema.
load_existing_log <- function(log_path) {
  if (!file.exists(log_path) || file.size(log_path) == 0) {
    return(empty_log_df())
  }

  # Repair header if needed before reading
  current_header <- readLines(log_path, n = 1, warn = FALSE)
  if (current_header != LOG_HEADER_LINE) {
    tryCatch(
      repair_mapping_log(log_path),
      error = function(e) {
        cli::cli_warn("Failed to repair log: {conditionMessage(e)}")
      }
    )
  }

  tryCatch(
    read.csv(log_path, stringsAsFactors = FALSE),
    error = function(e) empty_log_df()
  )
}

empty_log_df <- function() {

  data.frame(
    source_value = character(), domain = character(),
    concept_id = integer(), confidence = numeric(),
    stringsAsFactors = FALSE
  )
}

# Canonical 16-column header for the mapping improvement log.
LOG_HEADER_COLS <- c(
  "source_value", "domain", "record_count", "source_code", "source_name",
  "mapping_key", "concept_id", "concept_name", "vocabulary_id", "confidence",
  "reasoning", "source_is_ndc", "ndc_normalized", "tool_calls", "timestamp", "model"
)

LOG_HEADER_LINE <- paste(LOG_HEADER_COLS, collapse = ",")

# Repair a mapping improvement log CSV with misaligned columns.
# The log file may contain rows written by different code versions:
#   - 11-col (original):  sv, domain, record_count, concept_id, concept_name, vocabulary_id, confidence, reasoning, tool_calls, timestamp, model
#   - 13-col (v2, added source_is_ndc + ndc_normalized): same as 11 but inserts source_is_ndc, ndc_normalized between reasoning and tool_calls
#   - 16-col (v3, current): full format with source_code, source_name, mapping_key added between record_count and concept_id
# This function normalizes all rows to 16-column format and rewrites the file.
# Handles multi-line quoted fields (e.g., error messages with embedded newlines).
repair_mapping_log <- function(log_path) {
  if (!file.exists(log_path) || file.size(log_path) == 0) return(invisible(NULL))

  raw_lines <- readLines(log_path, warn = FALSE)
  if (length(raw_lines) < 2) return(invisible(NULL))  # header only

  # Step 1: Reassemble multi-line quoted fields into single logical lines.
  # A line is "incomplete" if it has an odd number of unescaped double quotes,
  # meaning a quoted field spans into the next physical line.
  logical_lines <- character()
  buffer <- ""
  in_multiline <- FALSE

  for (i in seq(2, length(raw_lines))) {
    line <- raw_lines[i]

    if (in_multiline) {
      # Continue assembling multi-line record
      buffer <- paste0(buffer, "\n", line)
    } else {
      buffer <- line
    }

    # Count unescaped quotes (double-double-quotes "" are escaped)
    stripped <- gsub('""', "", buffer, fixed = TRUE)
    n_quotes <- nchar(gsub('[^"]', "", stripped))

    if (n_quotes %% 2 == 0) {
      # Even quotes = complete CSV record
      if (nzchar(trimws(buffer))) {
        logical_lines <- c(logical_lines, buffer)
      }
      buffer <- ""
      in_multiline <- FALSE
    } else {
      # Odd quotes = record continues on next line
      in_multiline <- TRUE
    }
  }

  # Step 2: Parse each logical line and normalize column layout
  repaired <- vector("list", length(logical_lines))
  n_repaired <- 0L

  for (i in seq_along(logical_lines)) {
    line <- logical_lines[i]

    parsed <- tryCatch(
      read.csv(text = line, header = FALSE, stringsAsFactors = FALSE),
      error = function(e) NULL
    )
    if (is.null(parsed)) next

    ncols <- ncol(parsed)
    row <- as.character(parsed[1, ])

    if (ncols == 11L) {
      # Original 11-col format
      # Insert NA for: source_code, source_name, mapping_key, source_is_ndc, ndc_normalized
      new_row <- c(
        row[1:3],           # source_value, domain, record_count
        NA, NA, NA,          # source_code, source_name, mapping_key
        row[4:8],           # concept_id, concept_name, vocabulary_id, confidence, reasoning
        NA, NA,              # source_is_ndc, ndc_normalized
        row[9:11]           # tool_calls, timestamp, model
      )
    } else if (ncols == 13L) {
      # V2 13-col: adds source_is_ndc + ndc_normalized between reasoning and tool_calls
      # Insert NA for: source_code, source_name, mapping_key
      new_row <- c(
        row[1:3],           # source_value, domain, record_count
        NA, NA, NA,          # source_code, source_name, mapping_key
        row[4:13]           # concept_id through model
      )
    } else if (ncols == 16L) {
      # Current 16-col: already correct
      new_row <- row
    } else {
      # Unknown format — pad or truncate to 16
      if (ncols < 16L) {
        new_row <- c(row, rep(NA, 16L - ncols))
      } else {
        new_row <- row[1:16]
      }
    }

    n_repaired <- n_repaired + 1L
    repaired[[n_repaired]] <- new_row
  }

  if (n_repaired == 0L) return(invisible(NULL))

  # Step 3: Build repaired data.frame
  repaired_df <- as.data.frame(
    do.call(rbind, repaired[seq_len(n_repaired)]),
    stringsAsFactors = FALSE
  )
  names(repaired_df) <- LOG_HEADER_COLS

  # Coerce types
  repaired_df$record_count <- suppressWarnings(as.integer(repaired_df$record_count))
  repaired_df$concept_id <- suppressWarnings(as.integer(repaired_df$concept_id))
  repaired_df$confidence <- suppressWarnings(as.numeric(repaired_df$confidence))
  repaired_df$source_is_ndc <- suppressWarnings(as.logical(repaired_df$source_is_ndc))
  repaired_df$tool_calls <- suppressWarnings(as.integer(repaired_df$tool_calls))

  # Step 4: Rewrite file atomically
  tmp <- paste0(log_path, ".repair_tmp")
  writeLines(LOG_HEADER_LINE, tmp)
  write.table(repaired_df, tmp, append = TRUE, sep = ",",
              row.names = FALSE, col.names = FALSE, quote = TRUE, na = "NA")
  file.rename(tmp, log_path)

  cli::cli_alert_success("Repaired log file: {n_repaired} rows normalized to 16-column format.")
  invisible(repaired_df)
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
  # Default: only successfully mapped entries are skipped, so failed mappings

  # (concept_id = 0/NA or low confidence) are automatically retried.
  # force_retry = TRUE: retry everything (skip nothing)
  # force_retry = "all": skip all logged entries including failures
  if (identical(force_retry, "all")) {
    # Mark ALL logged entries as done (even failures won't retry)
    done_log <- existing_log
  } else {
    # Default: only mark successful entries as done (retry the failures)
    keep <- !is.na(existing_log$concept_id) &
      existing_log$concept_id > 0 &
      !is.na(existing_log$confidence) &
      existing_log$confidence >= confidence_threshold
    done_log <- existing_log[keep, , drop = FALSE]
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
    measurement_value = c("lab_results", "map_measurement_value"),
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
        '  TRIM(SUBSTR(COALESCE(NULLIF(TRIM(d.drug_ndc_normalized), \'\'), d.drug_name), 1, 50)) AS mapping_key ',
        'FROM "{stg}".map_drug_order d ',
        'JOIN "{stg}".medication_orders mo ',
        '  ON (d.drug_ndc_normalized = mo.drug_ndc_normalized ',
        '      OR (d.drug_ndc_normalized IS NULL AND mo.drug_ndc_normalized IS NULL)) ',
        '  AND d.drug_name = mo.drug_name ',
        'LEFT JOIN "{stg}".custom_concept_mapping cust ',
        '  ON cust.source_value = TRIM(SUBSTR(COALESCE(NULLIF(TRIM(d.drug_ndc_normalized), \'\'), d.drug_name), 1, 50)) AND cust.domain = \'drug\' ',
        'LEFT JOIN "{stg}".custom_ndc_mapping cndc ',
        '  ON cndc.drug_ndc_normalized = d.drug_ndc_normalized ',
        'WHERE COALESCE(NULLIF(d.drug_concept_id, 0), cust.concept_id, cndc.drug_concept_id, 0) = 0 ',
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
    measurement_value = {
      # Measurement Value: query lab_results for unmapped categorical result_description values.
      # Only non-numeric results (numeric_result IS NULL). Joins to map_measurement_value and
      # custom_concept_mapping with domain='measurement_value'. Also excludes values handled by
      # pattern-based fallback in the CDM load SQL (65_load_measurement_labs.sql).
      # source_code = test_loinc (context), source_name = test_name (context).
      sv <- "TRIM(SUBSTR(lr.result_description, 1, 50))"
      ne <- not_exists(sv, "measurement_value")
      glue::glue(
        'SELECT ',
        '  {sv} AS source_value, ',
        '  COUNT(*) AS record_count, ',
        '  lr.test_loinc AS source_code, ',
        '  lr.test_name AS source_name, ',
        '  \'Meas Value\' AS source_vocab, ',
        '  {sv} AS mapping_key ',
        'FROM "{stg}".lab_results lr ',
        'LEFT JOIN "{stg}".map_measurement_value mval ',
        '  ON mval.result_source_value = LOWER(TRIM(REPLACE(REPLACE(COALESCE(lr.result_description, \'\'), \'[\', \'\'), \']\', \'\'))) ',
        'LEFT JOIN "{stg}".custom_concept_mapping cust_val ',
        '  ON cust_val.source_value = TRIM(SUBSTR(lr.result_description, 1, 50)) AND cust_val.domain = \'measurement_value\' ',
        'WHERE COALESCE(mval.value_as_concept_id, cust_val.concept_id, 0) = 0 ',
        '  AND lr.result_description IS NOT NULL ',
        '  AND TRIM(lr.result_description) != \'\' ',
        '  AND lr.numeric_result IS NULL ',
        '  -- Exclude values handled by pattern-based fallback in CDM load SQL ',
        '  AND LOWER(lr.result_description) NOT LIKE \'%no abnormal%\' ',
        '  AND LOWER(lr.result_description) NOT LIKE \'%no lumps%\' ',
        '  AND LOWER(lr.result_description) NOT LIKE \'%no lump %\' ',
        '  AND LOWER(lr.result_description) NOT LIKE \'%no polyps%\' ',
        '  AND LOWER(lr.result_description) NOT LIKE \'%no growth%\' ',
        '  AND LOWER(lr.result_description) NOT LIKE \'%no pouches%\' ',
        '  AND LOWER(lr.result_description) NOT LIKE \'%no murmurs%\' ',
        '  AND LOWER(lr.result_description) NOT LIKE \'%no nasal%\' ',
        '  AND LOWER(lr.result_description) NOT LIKE \'%are normal%\' ',
        '  AND LOWER(lr.result_description) NOT LIKE \'%negative for%\' ',
        '  AND LOWER(lr.result_description) NOT LIKE \'%negative %\' ',
        '  {ne} ',
        'GROUP BY {sv}, lr.test_loinc, lr.test_name ',
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
  } else if (domain == "measurement_value") {
    parts <- c(parts, paste0("Result Value: \"", source_value, "\""))
    if (has_code) parts <- c(parts, paste0("Test LOINC (context): \"", source_code, "\""))
    if (has_name) parts <- c(parts, paste0("Test Name (context): \"", source_name, "\""))
    parts <- c(parts, paste0(
      "This is a categorical lab result value (not a test name). ",
      "Search for OMOP Meas Value concepts or SNOMED clinical findings ",
      "that represent this result (e.g., 'Normal', 'Positive', 'Pass'). ",
      "The test context is provided to help understand the meaning of the value."
    ))
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


# Map a single source value using the LLM tool-calling loop.
# Routes to openai_tool_loop() or anthropic_tool_loop() based on provider.
# Accepts rich context from staging: source_code, source_name, source_vocab.
# Returns list with concept_id, concept_name, vocabulary_id, confidence, reasoning, tool_calls.
map_single_value <- function(source_value, domain, record_count,
                             source_code = NA, source_name = NA, source_vocab = NA,
                             tools, tool_handlers, provider = "openai", model, api_key) {
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

  if (identical(provider, "anthropic")) {
    result <- anthropic_tool_loop(
      messages = messages,
      tools = tools,
      tool_handlers = tool_handlers,
      max_iterations = 10L,
      model = model,
      api_key = api_key
    )
  } else {
    result <- openai_tool_loop(
      messages = messages,
      tools = tools,
      tool_handlers = tool_handlers,
      max_iterations = 10L,
      model = model,
      api_key = api_key
    )
  }

  # Parse the final JSON response, retrying once if the LLM returned prose
  final_msg <- result$final_message
  if (!nzchar(trimws(final_msg))) {
    cli::cli_alert_warning("  LLM returned empty response after {result$tool_calls_made} tool call(s)")
    return(list(concept_id = NA_integer_, confidence = 0,
                reasoning = "LLM returned empty response", tool_calls = result$tool_calls_made))
  }

  parsed <- try_parse_llm_json(final_msg)

  # If parsing failed, send a follow-up asking the LLM to reformat as JSON
  if (is.null(parsed)) {
    cli::cli_alert_warning("  Non-JSON response, requesting JSON reformat...")
    retry_msg <- paste0(
      "Your response was not valid JSON. Please respond with ONLY the JSON object, ",
      "no other text:\n",
      "{\"concept_id\": <int or null>, \"concept_name\": \"...\", \"vocabulary_id\": \"...\", ",
      "\"confidence\": <0.0-1.0>, \"reasoning\": \"...\", ",
      "\"source_is_ndc\": <true/false>, \"ndc_normalized\": <string or null>}"
    )

    # Append the retry message to the conversation and make one more API call
    retry_messages <- c(result$messages, list(list(role = "user", content = retry_msg)))

    retry_result <- tryCatch({
      if (identical(provider, "openai")) {
        openai_chat(messages = retry_messages, tools = NULL,
                    model = model, api_key = api_key)
      } else {
        # For Anthropic, extract system prompt and pass separately
        sys <- NULL
        api_msgs <- list()
        for (m in retry_messages) {
          if (identical(m$role, "system")) { sys <- m$content } else { api_msgs <- c(api_msgs, list(m)) }
        }
        anthropic_chat(messages = api_msgs, tools = NULL,
                       model = model, api_key = api_key, system_prompt = sys)
      }
    }, error = function(e) NULL)

    if (!is.null(retry_result)) {
      retry_text <- if (identical(provider, "anthropic")) {
        text_blocks <- Filter(function(b) identical(b$type, "text"), retry_result$content)
        paste(vapply(text_blocks, function(b) b$text %||% "", character(1)), collapse = "\n")
      } else {
        retry_result$choices[[1]]$message$content %||% ""
      }
      parsed <- try_parse_llm_json(retry_text)
    }

    if (is.null(parsed)) {
      cli::cli_alert_warning("  Failed to get JSON after retry: {substr(final_msg, 1, 120)}")
      parsed <- list(concept_id = NA_integer_, confidence = 0,
                     reasoning = "Failed to parse LLM response")
    }
  }

  parsed$tool_calls <- result$tool_calls_made
  parsed
}

# Try to parse a JSON object from an LLM response string.
# Returns a parsed list on success, or NULL if no valid JSON found.
try_parse_llm_json <- function(text) {
  if (!nzchar(trimws(text))) return(NULL)

  # Try direct parse first
  parsed <- tryCatch(
    jsonlite::fromJSON(text, simplifyVector = FALSE),
    error = function(e) NULL
  )
  if (!is.null(parsed) && !is.null(parsed$concept_id)) return(parsed)

  # Strip markdown fences (```json ... ```)
  stripped <- gsub("```(?:json)?\\s*", "", text, perl = TRUE)
  stripped <- trimws(stripped)
  parsed <- tryCatch(
    jsonlite::fromJSON(stripped, simplifyVector = FALSE),
    error = function(e) NULL
  )
  if (!is.null(parsed) && !is.null(parsed$concept_id)) return(parsed)

  # Extract first JSON object from surrounding text
  m <- regmatches(text, regexpr("\\{[^{}]*(?:\\{[^{}]*\\}[^{}]*)*\\}", text, perl = TRUE))
  if (length(m) > 0 && nchar(m[1]) > 2) {
    parsed <- tryCatch(
      jsonlite::fromJSON(m[1], simplifyVector = FALSE),
      error = function(e) NULL
    )
    if (!is.null(parsed) && !is.null(parsed$concept_id)) return(parsed)
  }

  NULL
}


# --- Manual mapping helpers ---

#' Manually add custom concept mappings
#'
#' Adds one or more rows to \code{custom_concept_mapping.csv} (and optionally
#' \code{custom_ndc_mapping.csv} for drug NDC codes). Existing entries for the
#' same source_value + domain are updated in place.
#'
#' @section Usage:
#' \preformatted{
#' # Single mapping
#' add_custom_mapping("dexamethasone", "drug", 1518254)
#'
#' # With NDC
#' add_custom_mapping("dexamethasone", "drug", 1518254, ndc_code = "47202252901")
#'
#' # Batch — vectorized
#' add_custom_mapping(
#'   source_value = c("dexamethasone", "valproic acid", "nifedipine"),
#'   domain      = "drug",
#'   concept_id  = c(1518254, 789578, 1318137),
#'   ndc_code    = c("47202252901", "00093063201", "00047007824")
#' )
#'
#' # Non-drug domains
#' add_custom_mapping("Acute pharyngitis", "condition", 28060)
#' }
#'
#' @param source_value Character. The source value to map. For drugs, this
#'   should be the drug \strong{name} (not the NDC code), since the CDM load
#'   SQL joins custom_concept_mapping on drug_name.
#' @param domain Character. One of "condition", "drug", "measurement",
#'   "procedure", "observation".
#' @param concept_id Integer. The OMOP standard concept_id to map to.
#' @param custom_mapping_path Path to the custom_concept_mapping.csv file.
#'   Default: from config or package default.
#' @param ndc_code Optional character. For drugs, also write an entry to
#'   custom_ndc_mapping.csv with this NDC code.
#' @param custom_ndc_mapping_path Path to the custom_ndc_mapping.csv file.
#'   Default: from config or package default.
#' @return Invisible TRUE on success.
#' @export
add_custom_mapping <- function(source_value,
                               domain,
                               concept_id,
                               custom_mapping_path = NULL,
                               ndc_code = NULL,
                               custom_ndc_mapping_path = NULL) {
  # Validate inputs
  stopifnot(is.character(source_value), length(source_value) >= 1L)
  stopifnot(is.character(domain))
  stopifnot(is.numeric(concept_id))

  n <- length(source_value)

  # Recycle domain to match length of source_value
  if (length(domain) == 1L) domain <- rep(domain, n)
  stopifnot(length(domain) == n, length(concept_id) == n)

  valid_domains <- c("condition", "drug", "measurement", "measurement_value", "procedure", "observation")
  bad <- setdiff(unique(domain), valid_domains)
  if (length(bad) > 0) stop("Invalid domain(s): ", paste(bad, collapse = ", "), call. = FALSE)

  # Resolve paths
  config <- tryCatch(default_etl_config(), error = function(e) list())

  if (is.null(custom_mapping_path)) {
    custom_mapping_path <- config$custom_mapping_path
    if (is.null(custom_mapping_path) || !nzchar(trimws(custom_mapping_path))) {
      custom_mapping_path <- system.file("extdata", "custom_concept_mapping.csv", package = "ETLDelphi")
    }
  }

  # Read existing
  existing <- if (file.exists(custom_mapping_path)) {
    tryCatch(
      read.csv(custom_mapping_path, stringsAsFactors = FALSE),
      error = function(e) data.frame(source_value = character(), domain = character(), concept_id = integer(), stringsAsFactors = FALSE)
    )
  } else {
    data.frame(source_value = character(), domain = character(), concept_id = integer(), stringsAsFactors = FALSE)
  }

  new_rows <- data.frame(
    source_value = source_value,
    domain = domain,
    concept_id = as.integer(concept_id),
    stringsAsFactors = FALSE
  )

  merged <- rbind(existing, new_rows)
  dup_key <- paste(merged$source_value, merged$domain, sep = "|||")
  merged <- merged[!duplicated(dup_key, fromLast = TRUE), , drop = FALSE]
  write.csv(merged, custom_mapping_path, row.names = FALSE)

  cli::cli_alert_success("Added {n} mapping(s) to {custom_mapping_path}")

  # Handle NDC codes
  if (!is.null(ndc_code)) {
    if (length(ndc_code) == 1L) ndc_code <- rep(ndc_code, n)
    stopifnot(length(ndc_code) == n)

    # Only process non-NA NDC entries
    has_ndc <- !is.na(ndc_code) & nzchar(ndc_code)
    if (any(has_ndc)) {
      if (is.null(custom_ndc_mapping_path)) {
        custom_ndc_mapping_path <- config$custom_ndc_mapping_path
        if (is.null(custom_ndc_mapping_path) || !nzchar(trimws(custom_ndc_mapping_path))) {
          custom_ndc_mapping_path <- system.file("extdata", "custom_ndc_mapping.csv", package = "ETLDelphi")
        }
      }

      existing_ndc <- if (file.exists(custom_ndc_mapping_path)) {
        tryCatch(
          read.csv(custom_ndc_mapping_path, stringsAsFactors = FALSE),
          error = function(e) data.frame(drug_ndc_normalized = character(), drug_concept_id = integer(), stringsAsFactors = FALSE)
        )
      } else {
        data.frame(drug_ndc_normalized = character(), drug_concept_id = integer(), stringsAsFactors = FALSE)
      }

      new_ndc <- data.frame(
        drug_ndc_normalized = ndc_code[has_ndc],
        drug_concept_id = as.integer(concept_id[has_ndc]),
        stringsAsFactors = FALSE
      )

      merged_ndc <- rbind(existing_ndc, new_ndc)
      merged_ndc <- merged_ndc[!duplicated(merged_ndc$drug_ndc_normalized, fromLast = TRUE), , drop = FALSE]
      write.csv(merged_ndc, custom_ndc_mapping_path, row.names = FALSE)

      cli::cli_alert_success("Added {sum(has_ndc)} NDC mapping(s) to {custom_ndc_mapping_path}")
    }
  }

  invisible(TRUE)
}


#' Rebuild custom mapping CSVs from the mapping improvement log
#'
#' Scans the `mapping_improvement_log.csv` for successfully mapped values
#' (concept_id > 0 and confidence >= threshold) and writes them to
#' `custom_concept_mapping.csv` and `custom_ndc_mapping.csv`. Useful when
#' the custom CSV files have been reset or lost (e.g., by a package reinstall).
#'
#' Only the best (latest) mapping per source_value + domain pair is kept.
#' Existing entries in the CSV files are preserved; log entries take precedence
#' on conflict.
#'
#' @param log_path Path to the mapping improvement log CSV.
#' @param custom_mapping_path Path to custom_concept_mapping.csv. Default: package default.
#' @param custom_ndc_mapping_path Path to custom_ndc_mapping.csv. Default: package default.
#' @param confidence_threshold Minimum confidence to include. Default: 0.7.
#' @param dry_run If TRUE, show what would be written without modifying files.
#' @return Invisible list with counts: n_concept, n_ndc.
#' @export
rebuild_custom_mappings_from_log <- function(
    log_path = "mapping_improvement_log.csv",
    custom_mapping_path = NULL,
    custom_ndc_mapping_path = NULL,
    confidence_threshold = 0.7,
    dry_run = FALSE
) {
  if (!file.exists(log_path)) {
    cli::cli_alert_warning("Log file not found: {log_path}")
    return(invisible(list(n_concept = 0L, n_ndc = 0L)))
  }

  # Repair column alignment if header is outdated
  current_header <- readLines(log_path, n = 1, warn = FALSE)
  if (current_header != LOG_HEADER_LINE) {
    cli::cli_alert_warning("Log file header mismatch \u2014 repairing column alignment...")
    tryCatch(repair_mapping_log(log_path), error = function(e) {
      cli::cli_warn("Failed to repair log: {conditionMessage(e)}")
    })
  }

  log_df <- tryCatch(
    read.csv(log_path, stringsAsFactors = FALSE),
    error = function(e) {
      cli::cli_alert_danger("Failed to read log: {conditionMessage(e)}")
      return(data.frame())
    }
  )

  if (nrow(log_df) == 0) {
    cli::cli_alert_info("Log file is empty.")
    return(invisible(list(n_concept = 0L, n_ndc = 0L)))
  }

  # Filter to successful mappings
  good <- !is.na(log_df$concept_id) &
    log_df$concept_id > 0 &
    !is.na(log_df$confidence) &
    log_df$confidence >= confidence_threshold

  if (sum(good) == 0) {
    cli::cli_alert_info("No successful mappings found above confidence threshold {confidence_threshold}.")
    return(invisible(list(n_concept = 0L, n_ndc = 0L)))
  }

  good_df <- log_df[good, , drop = FALSE]
  cli::cli_alert_info("Found {nrow(good_df)} successful mapping(s) in log (confidence >= {confidence_threshold}).")

  # Resolve paths
  config <- tryCatch(default_etl_config(), error = function(e) list())

  if (is.null(custom_mapping_path)) {
    custom_mapping_path <- config$custom_mapping_path
    if (is.null(custom_mapping_path) || !nzchar(trimws(custom_mapping_path))) {
      custom_mapping_path <- system.file("extdata", "custom_concept_mapping.csv", package = "ETLDelphi")
    }
  }

  if (is.null(custom_ndc_mapping_path)) {
    custom_ndc_mapping_path <- config$custom_ndc_mapping_path
    if (is.null(custom_ndc_mapping_path) || !nzchar(trimws(custom_ndc_mapping_path))) {
      custom_ndc_mapping_path <- system.file("extdata", "custom_ndc_mapping.csv", package = "ETLDelphi")
    }
  }

  # --- Build concept mappings ---
  # Use mapping_key if available, otherwise source_value
  mk_col <- if ("mapping_key" %in% names(good_df)) good_df$mapping_key else good_df$source_value
  mk_col <- ifelse(is.na(mk_col) | !nzchar(mk_col), good_df$source_value, mk_col)

  concept_rows <- data.frame(
    source_value = mk_col,
    domain = good_df$domain,
    concept_id = as.integer(good_df$concept_id),
    stringsAsFactors = FALSE
  )

  # De-duplicate: keep latest (last) per source_value + domain
  dup_key <- paste(concept_rows$source_value, concept_rows$domain, sep = "|||")
  concept_rows <- concept_rows[!duplicated(dup_key, fromLast = TRUE), , drop = FALSE]

  # --- Build NDC mappings ---
  ndc_col <- if ("ndc_normalized" %in% names(good_df)) good_df$ndc_normalized else NA_character_
  is_ndc <- if ("source_is_ndc" %in% names(good_df)) as.logical(good_df$source_is_ndc) else rep(FALSE, nrow(good_df))
  is_ndc[is.na(is_ndc)] <- FALSE

  has_ndc <- is_ndc & good_df$domain == "drug" & !is.na(ndc_col) & nzchar(ndc_col)
  ndc_rows <- if (any(has_ndc)) {
    data.frame(
      drug_ndc_normalized = ndc_col[has_ndc],
      drug_concept_id = as.integer(good_df$concept_id[has_ndc]),
      stringsAsFactors = FALSE
    )
  } else {
    data.frame(drug_ndc_normalized = character(), drug_concept_id = integer(), stringsAsFactors = FALSE)
  }
  ndc_rows <- ndc_rows[!duplicated(ndc_rows$drug_ndc_normalized, fromLast = TRUE), , drop = FALSE]

  # --- Summary ---
  cli::cli_alert_info("Would write {nrow(concept_rows)} concept mapping(s) and {nrow(ndc_rows)} NDC mapping(s).")

  by_domain <- table(concept_rows$domain)
  for (d in names(by_domain)) {
    cli::cli_bullets(c("*" = "{d}: {by_domain[[d]]} mapping(s)"))
  }

  if (dry_run) {
    cli::cli_alert_info("Dry run — no files modified.")
    return(invisible(list(n_concept = nrow(concept_rows), n_ndc = nrow(ndc_rows))))
  }

  # --- Merge with existing and write ---
  # Concept mappings
  existing_concept <- if (file.exists(custom_mapping_path)) {
    tryCatch(read.csv(custom_mapping_path, stringsAsFactors = FALSE), error = function(e) {
      data.frame(source_value = character(), domain = character(), concept_id = integer(), stringsAsFactors = FALSE)
    })
  } else {
    data.frame(source_value = character(), domain = character(), concept_id = integer(), stringsAsFactors = FALSE)
  }

  merged <- rbind(existing_concept, concept_rows)
  merged_key <- paste(merged$source_value, merged$domain, sep = "|||")
  merged <- merged[!duplicated(merged_key, fromLast = TRUE), , drop = FALSE]
  write.csv(merged, custom_mapping_path, row.names = FALSE)
  cli::cli_alert_success("Wrote {nrow(merged)} concept mapping(s) to {custom_mapping_path}")

  # NDC mappings
  if (nrow(ndc_rows) > 0) {
    existing_ndc <- if (file.exists(custom_ndc_mapping_path)) {
      tryCatch(read.csv(custom_ndc_mapping_path, stringsAsFactors = FALSE), error = function(e) {
        data.frame(drug_ndc_normalized = character(), drug_concept_id = integer(), stringsAsFactors = FALSE)
      })
    } else {
      data.frame(drug_ndc_normalized = character(), drug_concept_id = integer(), stringsAsFactors = FALSE)
    }
    merged_ndc <- rbind(existing_ndc, ndc_rows)
    merged_ndc <- merged_ndc[!duplicated(merged_ndc$drug_ndc_normalized, fromLast = TRUE), , drop = FALSE]
    write.csv(merged_ndc, custom_ndc_mapping_path, row.names = FALSE)
    cli::cli_alert_success("Wrote {nrow(merged_ndc)} NDC mapping(s) to {custom_ndc_mapping_path}")
  }

  invisible(list(n_concept = nrow(concept_rows), n_ndc = nrow(ndc_rows)))
}
