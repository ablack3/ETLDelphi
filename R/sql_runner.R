# Check if table exists in schema (avoids DBI::Id which can trigger cli progress bar bugs on some setups)
table_exists <- function(con, schema, table) {
  q <- glue::glue(
    "SELECT 1 FROM information_schema.tables WHERE table_schema = '{gsub(\"'\", \"''\", schema)}' AND table_name = '{gsub(\"'\", \"''\", table)}'"
  )
  nrow(DBI::dbGetQuery(con, q)) > 0
}

#' Execute SQL scripts in order with logging
#'
#' Runs SQL files from a directory in lexicographic order (by folder then file name).
#' Logs each step and supports dry_run, from_step, to_step.
#'
#' @param con DBI connection (e.g. duckdb).
#' @param sql_dir Path to directory containing ordered subdirs (00_admin, 10_stage, ...).
#' @param config List with at least \code{schemas} (stg, cdm).
#' @param dry_run If TRUE, only list files that would be run.
#' @param from_step Optional character: start from this step (folder or file name prefix).
#' @param to_step Optional character: stop after this step.
#' @param run_id Optional integer for etl_run_log; if NULL, one is created.
#' @return Invisible list with run_id, steps run, and any error info.
#' @export
run_sql_scripts <- function(con,
                            sql_dir,
                            config,
                            dry_run = FALSE,
                            from_step = NULL,
                            to_step = NULL,
                            run_id = NULL) {
  if (!dir.exists(sql_dir)) {
    stop(glue::glue("SQL directory not found: {sql_dir}"))
  }

  # Collect all .sql files in lexicographic order
  files <- sort(fs::dir_ls(sql_dir, recurse = TRUE, type = "file", glob = "*.sql"))
  if (length(files) == 0) {
    cli::cli_warn("No .sql files found in {sql_dir}")
    return(invisible(list(run_id = run_id, steps = character(0))))
  }

  # Relative path for step name
  rel_paths <- fs::path_rel(files, sql_dir)
  if (!is.null(from_step)) {
    idx <- which(startsWith(rel_paths, from_step) | startsWith(basename(rel_paths), from_step))
    if (length(idx) > 0) files <- files[min(idx):length(files)]
    rel_paths <- fs::path_rel(files, sql_dir)
  }
  if (!is.null(to_step)) {
    idx <- which(startsWith(rel_paths, to_step) | startsWith(basename(rel_paths), to_step))
    if (length(idx) > 0) files <- files[1:max(idx)]
  }

  if (dry_run) {
    cli::cli_alert_info("Dry run: would execute {length(files)} file(s)")
    for (f in fs::path_rel(files, sql_dir)) cli::cli_bullets(c("*" = f))
    return(invisible(list(dry_run = TRUE, files = fs::path_rel(files, sql_dir))))
  }

  stg <- config[["schemas"]][["stg"]]; if (is.null(stg)) stg <- "stg"
  started_at <- Sys.time()
  if (is.null(run_id)) {
    run_id <- as.integer(as.numeric(Sys.time()))
  }

  # Optionally insert run log (if 01_etl_log has been run)
  try_insert_run <- function() {
    if (table_exists(con, stg, "etl_run_log")) {
      DBI::dbExecute(con, glue::glue("INSERT INTO \"{stg}\".etl_run_log (run_id, started_at, status) VALUES ({run_id}, '{format(started_at, '%Y-%m-%d %H:%M:%S')}', 'running')"))
    }
  }
  tryCatch(try_insert_run(), error = function(e) NULL)

  steps_done <- character(0)
  for (i in seq_along(files)) {
    f <- files[[i]]
    step_name <- fs::path_rel(f, sql_dir)
    sql <- readLines(f, warn = FALSE)
    sql <- paste(sql, collapse = "\n")
    # Substitute schema placeholders if used
    cdm <- config[["schemas"]][["cdm"]]; if (is.null(cdm)) cdm <- "cdm"
    src <- config[["schemas"]][["src"]]; if (is.null(src)) src <- "src"
    sql <- gsub("@cdmDatabaseSchema", cdm, sql, fixed = TRUE)
    sql <- gsub("\\{cdm\\}", cdm, sql)
    sql <- gsub("\\{stg\\}", stg, sql)
    sql <- gsub("\\{src\\}", src, sql)
    # Drug name mapping path (Hecate-built CSV for fallback drug mapping)
    drug_path <- config[["drug_name_mapping_path"]]
    if (is.null(drug_path) || !nzchar(trimws(drug_path))) {
      drug_path <- system.file("extdata", "drug_name_to_concept.csv", package = "ETLDelphi")
    }
    if (nzchar(drug_path) && file.exists(drug_path)) {
      drug_path_final <- normalizePath(drug_path, mustWork = TRUE)
    } else {
      # No mapping file: use temp file with header only (empty mapping)
      tmp <- tempfile(fileext = ".csv")
      writeLines("drug_name,concept_id", tmp)
      on.exit(unlink(tmp), add = TRUE)
      drug_path_final <- normalizePath(tmp, mustWork = TRUE)
    }
    sql <- gsub("@drugNameMappingPath", drug_path_final, sql, fixed = TRUE)
    # Strip single-line comments so semicolons in comments don't break statement split
    sql <- gsub("--[^\n]*", "\n", sql)

    cli::cli_alert_info("Running {step_name}")
    step_start <- Sys.time()
    rows_affected <- NA_integer_
    status <- "ok"
    notes <- NA_character_

    tryCatch({
      # DuckDB can run multiple statements; DBI::dbExecute runs one at a time
      statements <- split_sql_statements(sql)
      for (stmt in statements) {
        stmt <- trimws(stmt)
        if (nchar(stmt) == 0) next
        res <- DBI::dbExecute(con, stmt)
        if (is.integer(res) && length(res) == 1) rows_affected <- res
      }
      steps_done <- c(steps_done, step_name)
    }, error = function(e) {
      status <<- "error"
      notes <<- conditionMessage(e)
      msg <- conditionMessage(e)
      # Escape braces in error message so cli does not re-evaluate (avoids pb_cur etc. in nested messages)
      msg_safe <- gsub("\\{", "{{", gsub("\\}", "}}", msg, fixed = TRUE), fixed = TRUE)
      cli::cli_abort("ETL failed at {step_name}: {msg_safe}", call = NULL)
    })

    step_end <- Sys.time()
    if (table_exists(con, stg, "etl_step_log")) {
      tryCatch({
        notes_s <- if (is.na(notes)) "NULL" else paste0("'", gsub("'", "''", notes), "'")
        rows_s <- if (is.na(rows_affected)) "NULL" else as.character(rows_affected)
        DBI::dbExecute(con, glue::glue("INSERT INTO \"{stg}\".etl_step_log (run_id, step_name, file_name, started_at, ended_at, status, rows_affected, notes) VALUES ({run_id}, '{gsub(\"'\", \"''\", step_name)}', '{basename(f)}', '{format(step_start, '%Y-%m-%d %H:%M:%S')}', '{format(step_end, '%Y-%m-%d %H:%M:%S')}', '{status}', {rows_s}, {notes_s})"))
      }, error = function(e) NULL)
    }
  }

  # Mark run complete
  try_update_run <- function() {
    if (table_exists(con, stg, "etl_run_log")) {
      DBI::dbExecute(con, glue::glue("UPDATE \"{stg}\".etl_run_log SET ended_at = '{format(Sys.time(), '%Y-%m-%d %H:%M:%S')}', status = 'completed' WHERE run_id = {run_id}"))
    }
  }
  tryCatch(try_update_run(), error = function(e) NULL)

  invisible(list(run_id = run_id, steps = steps_done))
}

#' Split SQL script into single statements (semicolon-terminated, ignore in strings)
split_sql_statements <- function(sql) {
  sql <- paste(sql, collapse = "\n")
  # Simple split on semicolon followed by newline or end (avoid splitting in strings)
  parts <- strsplit(sql, ";(?=(?:[^']*'[^']*')*[^']*$)", perl = TRUE)[[1]]
  parts <- trimws(parts)
  parts[nchar(parts) > 0]
}
