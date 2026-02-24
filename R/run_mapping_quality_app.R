#' Launch the mapping quality dashboard
#'
#' Serves the static HTML mapping quality dashboard using a local HTTP
#' server. Run after \code{\link{analyze_mapping_quality}} has written
#' \code{mapping_quality.json} into \code{results_dir}.
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
run_mapping_quality_app <- function(results_dir = "mapping_quality_results",
                                    port = 0L,
                                    launch_browser = TRUE) {
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
  cli::cli_alert_success("Dashboard running at {.url {url}}")
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
