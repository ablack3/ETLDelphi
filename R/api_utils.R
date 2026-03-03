# api_utils.R
# Shared HTTP request/response utilities for API clients (Hecate, OMOPHub).
# Internal helpers - not exported.

# Build an httr2 request for an API endpoint.
#
# @param client A client object with `base_url`, `timeout_ms`, and `api_key`.
# @param path API endpoint path (leading slashes stripped).
# @param query Named list of query parameters (NULLs filtered out).
# @param retry Logical; if TRUE, add 3-try retry with exponential backoff.
# @return An httr2 request object.
api_build_request <- function(client, path, query = NULL, retry = FALSE) {
  url <- paste0(client$base_url, "/", sub("^/+", "", path))

  req <- httr2::request(url) |>
    httr2::req_timeout(client$timeout_ms / 1000)

  if (retry) {
    req <- httr2::req_retry(req, max_tries = 3, backoff = ~ 2)
  }

  if (nzchar(client$api_key)) {
    req <- httr2::req_headers(req, Authorization = paste("Bearer", client$api_key))
  }

  if (!is.null(query)) {
    query <- Filter(Negate(is.null), query)
    req <- httr2::req_url_query(req, !!!query)
  }

  req
}

# Perform an API request with error handling. Returns parsed JSON as list.
#
# @param req An httr2 request object.
# @return Parsed list from JSON response body, or list with `error` key on failure.
api_perform <- function(req) {
  resp <- tryCatch(httr2::req_perform(req), error = function(e) e)

  if (inherits(resp, "error")) {
    return(list(error = "request_failed", message = conditionMessage(resp)))
  }

  status <- httr2::resp_status(resp)
  body_txt <- httr2::resp_body_string(resp)
  parsed <- tryCatch(jsonlite::fromJSON(body_txt, simplifyVector = FALSE), error = function(e) NULL)

  if (status >= 400) {
    return(list(
      error = "api_error",
      status = status,
      body = parsed %||% body_txt
    ))
  }

  parsed %||% list(raw = body_txt)
}
