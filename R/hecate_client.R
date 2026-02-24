# hecate_client.R
# Hecate API client for OMOP vocabulary search. Adapted from extras/hecate_client.R
# for use as a proper package dependency. Config via env vars HECATE_BASE_URL and
# HECATE_API_KEY, or by passing arguments directly.

#' Create a Hecate API client
#'
#' @param base_url Hecate API base URL. Default: env var HECATE_BASE_URL or
#'   \code{"https://hecate.pantheon-hds.com/api"}.
#' @param api_key Hecate API key. Default: env var HECATE_API_KEY.
#' @param timeout_ms Request timeout in milliseconds. Default: 10000.
#' @return A \code{hecate_client} object (list with class).
#' @export
hecate_client <- function(
    base_url = Sys.getenv("HECATE_BASE_URL", "https://hecate.pantheon-hds.com/api"),
    api_key = Sys.getenv("HECATE_API_KEY", ""),
    timeout_ms = 10000L) {
  structure(
    list(
      base_url = sub("/+$", "", base_url),
      timeout_ms = timeout_ms,
      api_key = api_key
    ),
    class = "hecate_client"
  )
}

# Hecate uses shared api_build_request() / api_perform() from api_utils.R

# --- Exported wrappers (return parsed R lists) ---

#' Search OMOP vocabulary concepts via Hecate
#'
#' @param query Search text (concept name, code, or description).
#' @param vocabulary_id Optional vocabulary filter (e.g. \code{"SNOMED"}, \code{"RxNorm"}).
#' @param domain_id Optional domain filter (e.g. \code{"Condition"}, \code{"Drug"}).
#' @param standard_concept Optional: \code{"S"} for standard, \code{"C"} for classification.
#' @param concept_class_id Optional concept class filter.
#' @param limit Max results (default 25).
#' @param client A \code{hecate_client} object; created from env vars if NULL.
#' @return Parsed list of search results.
#' @export
hecate_search <- function(query,
                          vocabulary_id = NULL,
                          domain_id = NULL,
                          standard_concept = NULL,
                          concept_class_id = NULL,
                          limit = 25L,
                          client = NULL) {
  client <- client %||% hecate_client()

  if (!is.character(query) || length(query) != 1 || is.na(query) || nchar(query) < 1) {
    stop("`query` must be a non-empty string.")
  }

  req <- api_build_request(
    client,
    path = "search",
    query = list(
      q = query,
      vocabulary_id = vocabulary_id,
      standard_concept = standard_concept,
      domain_id = domain_id,
      concept_class_id = concept_class_id,
      limit = limit
    )
  )

  api_perform(req)
}

#' Get a concept by ID from Hecate
#'
#' @param concept_id Integer OMOP concept_id.
#' @param client A \code{hecate_client} object; created from env vars if NULL.
#' @return Parsed concept details (list).
#' @export
hecate_get_concept <- function(concept_id, client = NULL) {
  client <- client %||% hecate_client()
  concept_id <- as.integer(concept_id)
  if (is.na(concept_id) || concept_id < 1) stop("`concept_id` must be a positive integer.")

  req <- api_build_request(client, path = paste0("concepts/", concept_id))
  res <- api_perform(req)

  # API may return an array; extract first element
  if (is.list(res) && is.null(res$error) && length(res) >= 1 && is.list(res[[1]])) {
    res[[1]]
  } else {
    res
  }
}

#' Get concept relationships from Hecate
#'
#' @param concept_id Integer OMOP concept_id.
#' @param client A \code{hecate_client} object; created from env vars if NULL.
#' @return Parsed relationship list.
#' @export
hecate_get_relationships <- function(concept_id, client = NULL) {
  client <- client %||% hecate_client()
  concept_id <- as.integer(concept_id)
  if (is.na(concept_id) || concept_id < 1) stop("`concept_id` must be a positive integer.")

  req <- api_build_request(client, path = paste0("concepts/", concept_id, "/relationships"))
  api_perform(req)
}
