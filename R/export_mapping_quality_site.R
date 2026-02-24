#' Export mapping quality metrics as JSON
#'
#' Runs all mapping quality queries against the DuckDB database and writes
#' a single JSON file containing all metrics. This is the data file consumed
#' by the static HTML dashboard (\code{inst/site/index.html}).
#'
#' @param con DBI connection to the DuckDB database (post-ETL).
#' @param output_path Path for the JSON output file.
#' @param config Optional ETL config list. Default: \code{NULL} (uses \code{stg}/\code{main} schemas).
#' @return Invisible path to the written JSON file.
#' @export
export_mapping_quality_json <- function(con, output_path = "mapping_quality.json", config = NULL) {
  data <- run_mapping_quality_queries(con, config)
  data$generated_at <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

  json <- jsonlite::toJSON(data, pretty = TRUE, auto_unbox = TRUE, dataframe = "rows")
  writeLines(json, output_path)
  cli::cli_alert_success("Wrote {output_path}")

  invisible(output_path)
}


#' Export a complete static mapping quality site
#'
#' Assembles a deployable directory containing \code{index.html} and
#' \code{mapping_quality.json}. Open \code{index.html} in a browser or
#' push the directory to GitHub Pages.
#'
#' The preferred workflow is to pass \code{json_path} pointing at the
#' \code{mapping_quality.json} file already written by
#' \code{\link{analyze_mapping_quality}}. As a convenience you can still
#' pass a DBI \code{con} to query the database directly.
#'
#' @param json_path Path to an existing \code{mapping_quality.json} file
#'   (e.g. as written by \code{analyze_mapping_quality()}).
#' @param outdir Directory to write the site into. Created if missing.
#' @param con Optional DBI connection to the DuckDB database.
#'   Used only when \code{json_path} is \code{NULL}.
#' @param config Optional ETL config list. Only used with \code{con}.
#' @return Invisible path to \code{outdir}.
#' @export
export_mapping_quality_site <- function(json_path = NULL,
                                        outdir = "docs",
                                        con = NULL,
                                        config = NULL) {
  if (!dir.exists(outdir)) {
    dir.create(outdir, recursive = TRUE)
  }

  if (!is.null(json_path)) {
    # Primary path: copy pre-existing JSON
    if (!file.exists(json_path)) {
      stop("JSON file not found: ", json_path, call. = FALSE)
    }
    file.copy(json_path, file.path(outdir, "mapping_quality.json"), overwrite = TRUE)
  } else if (!is.null(con)) {
    # Convenience path: query DB directly
    export_mapping_quality_json(con, file.path(outdir, "mapping_quality.json"), config)
  } else {
    stop("Provide either json_path or con.", call. = FALSE)
  }

  # Copy static HTML
  html_src <- system.file("site", "index.html", package = "ETLDelphi")
  if (!nzchar(html_src) || !file.exists(html_src)) {
    stop("Static site template not found. Ensure inst/site/index.html exists in the ETLDelphi package.", call. = FALSE)
  }
  file.copy(html_src, file.path(outdir, "index.html"), overwrite = TRUE)

  cli::cli_alert_success("Site exported to {outdir}/")
  cli::cli_alert_info("Open {file.path(outdir, 'index.html')} in a browser, or push to GitHub Pages.")

  invisible(outdir)
}


#' Serve the static mapping quality dashboard
#'
#' Launches a local HTTP server (via \pkg{httpuv}) to serve the static HTML
#' mapping quality dashboard. Run after \code{\link{analyze_mapping_quality}}
#' has written \code{mapping_quality.json} into \code{results_dir}.
#'
#' For the full interactive Shiny dashboard, see
#' \code{\link{run_mapping_quality_app}}.
#'
#' @param results_dir Path to the folder containing \code{mapping_quality.json}
#'   (written by \code{analyze_mapping_quality()}).
#'   Defaults to \code{"mapping_quality_results"}.
#' @param port Port number for the local HTTP server. Default \code{0L} picks a
#'   random available port.
#' @param launch_browser If \code{TRUE} (default), open the dashboard in the
#'   system browser.
#' @return Invisible \code{NULL} (blocks until the server is stopped with
#'   Ctrl+C or Esc).
#' @export
run_mapping_quality_site <- function(results_dir = "mapping_quality_results",
                                     port = 0L,
                                     launch_browser = TRUE) {
  if (!requireNamespace("httpuv", quietly = TRUE)) {
    stop(
      "Package 'httpuv' is required to serve the static dashboard. ",
      "Install it with: install.packages('httpuv')",
      call. = FALSE
    )
  }

  # ── Resolve results_dir ──────────────────────────────────────────
  if (is.null(results_dir) || !nzchar(trimws(results_dir))) {
    results_dir <- "mapping_quality_results"
  }
  results_dir <- normalizePath(results_dir, mustWork = FALSE, winslash = "/")

  json_path <- file.path(results_dir, "mapping_quality.json")
  if (!file.exists(json_path)) {
    stop(
      "mapping_quality.json not found in '", results_dir, "'. ",
      "Run analyze_mapping_quality() first.",
      call. = FALSE
    )
  }

  # ── Locate the bundled HTML template ─────────────────────────────
  html_src <- system.file("site", "index.html", package = "ETLDelphi")
  if (!nzchar(html_src) || !file.exists(html_src)) {
    stop(
      "Static dashboard not found. ",
      "Ensure inst/site/index.html exists in the ETLDelphi package.",
      call. = FALSE
    )
  }

  # ── Assemble serving directory ───────────────────────────────────
  serve_dir <- tempfile("mq_dashboard_")
  dir.create(serve_dir)
  file.copy(html_src, file.path(serve_dir, "index.html"))
  file.copy(json_path, file.path(serve_dir, "mapping_quality.json"))
  on.exit(unlink(serve_dir, recursive = TRUE), add = TRUE)

  # ── Start httpuv static server ───────────────────────────────────
  server <- httpuv::startServer(
    host = "127.0.0.1",
    port = port,
    app = list(
      call = function(req) {
        path <- req$PATH_INFO
        if (path == "" || path == "/") path <- "/index.html"
        file_path <- file.path(serve_dir, sub("^/", "", path))

        if (!file.exists(file_path)) {
          return(list(
            status = 404L,
            headers = list("Content-Type" = "text/plain"),
            body = "Not found"
          ))
        }

        ext <- tolower(tools::file_ext(file_path))
        content_type <- switch(ext,
          html = "text/html; charset=utf-8",
          json = "application/json; charset=utf-8",
          css  = "text/css",
          js   = "application/javascript",
          "application/octet-stream"
        )

        list(
          status = 200L,
          headers = list(
            "Content-Type" = content_type,
            "Cache-Control" = "no-cache"
          ),
          body = readBin(file_path, "raw", file.info(file_path)$size)
        )
      }
    )
  )
  on.exit(httpuv::stopServer(server), add = TRUE)

  url <- paste0("http://127.0.0.1:", server$getPort(), "/")
  cli::cli_alert_success("Static dashboard running at {.url {url}}")
  cli::cli_alert_info("Press Ctrl+C or Esc to stop the server.")

  if (launch_browser) {
    utils::browseURL(url)
  }

  # Block until interrupted (same pattern as shiny::runApp)
  tryCatch(
    while (TRUE) {
      httpuv::service(timeoutMs = 1000)
    },
    interrupt = function(e) {
      cli::cli_alert_info("Server stopped.")
    }
  )

  invisible(NULL)
}
