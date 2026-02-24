# omophub_client.R
# OMOPHub API client for OMOP vocabulary lookups, especially NDC code resolution.
# Complements the Hecate client: OMOPHub excels at exact code lookups (by-code endpoint)
# while Hecate excels at semantic/string search via its vector DB.
# Config via env vars OMOPHUB_API_KEY (required) and OMOPHUB_BASE_URL.

#' Create an OMOPHub API client
#'
#' @param base_url OMOPHub API base URL. Default: env var OMOPHUB_BASE_URL or
#'   \code{"https://api.omophub.com/v1"}.
#' @param api_key OMOPHub API key (prefixed \code{oh_}). Default: env var OMOPHUB_API_KEY.
#' @param timeout_ms Request timeout in milliseconds. Default: 15000.
#' @return An \code{omophub_client} object (list with class).
#' @export
omophub_client <- function(
    base_url = Sys.getenv("OMOPHUB_BASE_URL", "https://api.omophub.com/v1"),
    api_key = Sys.getenv("OMOPHUB_API_KEY", ""),
    timeout_ms = 15000L) {
  structure(
    list(
      base_url = sub("/+$", "", base_url),
      timeout_ms = timeout_ms,
      api_key = api_key
    ),
    class = "omophub_client"
  )
}

# OMOPHub uses shared api_build_request() / api_perform() from api_utils.R
# OMOPHub requests use retry = TRUE for resilience against transient failures.

# --- Exported wrappers ---

#' Look up an OMOP concept by vocabulary and code
#'
#' Direct code lookup, ideal for NDC codes. Returns the concept matching the
#' exact code in the specified vocabulary.
#'
#' @param vocabulary_id Vocabulary (e.g. \code{"NDC"}, \code{"RxNorm"}, \code{"ICD10CM"}).
#' @param concept_code The code to look up (e.g. \code{"00069015001"}).
#' @param client An \code{omophub_client} object; created from env vars if NULL.
#' @return Parsed concept details (list), or list with \code{error} on failure.
#' @export
omophub_get_by_code <- function(vocabulary_id, concept_code, client = NULL) {
  client <- client %||% omophub_client()
  if (!nzchar(client$api_key)) stop("OMOPHUB_API_KEY not set.", call. = FALSE)

  path <- paste0("concepts/by-code/", vocabulary_id, "/", concept_code)
  req <- api_build_request(client, path, retry = TRUE)
  api_perform(req)
}

#' Search OMOPHub concepts
#'
#' Basic concept search with optional vocabulary and domain filters.
#'
#' @param query Search text.
#' @param vocabulary_ids Optional: comma-separated vocabulary IDs (e.g. \code{"NDC,RxNorm"}).
#' @param domain_ids Optional: comma-separated domain IDs (e.g. \code{"Drug"}).
#' @param page_size Max results (default 25).
#' @param client An \code{omophub_client} object; created from env vars if NULL.
#' @return Parsed search results (list with \code{data} and \code{meta}).
#' @export
omophub_search <- function(query, vocabulary_ids = NULL, domain_ids = NULL,
                           page_size = 25L, client = NULL) {
  client <- client %||% omophub_client()
  if (!nzchar(client$api_key)) stop("OMOPHUB_API_KEY not set.", call. = FALSE)

  req <- api_build_request(
    client,
    path = "search/concepts",
    query = list(
      query = query,
      vocabulary_ids = vocabulary_ids,
      domain_ids = domain_ids,
      page_size = page_size
    ),
    retry = TRUE
  )
  api_perform(req)
}

#' Map source codes to a target vocabulary via OMOPHub
#'
#' Batch mapping endpoint. Maps source codes (e.g. NDC) to a target vocabulary
#' (e.g. RxNorm) in a single call. Follows \code{"Maps to"} relationships.
#'
#' @param source_codes List of lists, each with \code{vocabulary_id} and \code{concept_code}.
#' @param target_vocabulary Target vocabulary (e.g. \code{"RxNorm"}).
#' @param client An \code{omophub_client} object; created from env vars if NULL.
#' @return Parsed mapping results (list).
#' @export
omophub_map_codes <- function(source_codes, target_vocabulary = "RxNorm", client = NULL) {
  client <- client %||% omophub_client()
  if (!nzchar(client$api_key)) stop("OMOPHUB_API_KEY not set.", call. = FALSE)

  body <- list(
    target_vocabulary = target_vocabulary,
    source_codes = source_codes
  )

  req <- api_build_request(client, path = "concepts/map", retry = TRUE) |>
    httr2::req_body_json(body, auto_unbox = TRUE)

  api_perform(req)
}
