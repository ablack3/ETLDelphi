# hecate_client.R
# Hecate API client for OMOP vocabulary search. Use when you need to look up
# concepts, e.g. for unmapped units, drug codes, or condition mappings.
#
# Usage (interactive):
#   source("extras/hecate_client.R")
#   vocabulary_search("gram")
#   vocabulary_search("iu", vocabulary_id = "UCUM", limit = 10)
#
# Usage (CLI for AI assistant):
#   Rscript -e "source('extras/hecate_client.R'); vocabulary_search('gram')"

library(dplyr)
library(httr2)
library(jsonlite)

`%||%` <- function(a, b) if (!is.null(a)) a else b

hecate_client <- function(
    base_url = Sys.getenv("HECATE_BASE_URL", "https://hecate.pantheon-hds.com/api"),
    timeout_ms = as.integer(Sys.getenv("HECATE_TIMEOUT_MS", "10000")),
    api_key = Sys.getenv("HECATE_API_KEY", "")
) {
  structure(
    list(
      base_url = sub("/+$", "", base_url),
      timeout_ms = timeout_ms,
      api_key = api_key
    ),
    class = "hecate_client"
  )
}

hecate_request <- function(client, path, query = NULL) {
  url <- paste0(client$base_url, "/", sub("^/+", "", path))

  req <- request(url) |>
    req_timeout(client$timeout_ms / 1000)

  if (nzchar(client$api_key)) {
    req <- req_headers(req, Authorization = paste("Bearer", client$api_key))
  }

  if (!is.null(query)) {
    query <- Filter(Negate(is.null), query)
    req <- req_url_query(req, !!!query)
  }

  req
}

hecate_perform <- function(req) {
  resp <- tryCatch(req_perform(req), error = function(e) e)

  if (inherits(resp, "error")) {
    return(list(error = "request_failed", message = conditionMessage(resp)))
  }

  status <- resp_status(resp)
  body_txt <- resp_body_string(resp)
  parsed <- tryCatch(fromJSON(body_txt, simplifyVector = FALSE), error = function(e) NULL)

  if (status >= 400) {
    return(list(
      error = "api_error",
      status = status,
      status_text = resp_status_desc(resp),
      body = parsed %||% body_txt
    ))
  }

  parsed %||% list(raw = body_txt)
}

assert_string <- function(x, name, min = 0, max = Inf) {
  if (!is.character(x) || length(x) != 1 || is.na(x)) stop(sprintf("`%s` must be a single string.", name))
  if (nchar(x) < min) stop(sprintf("`%s` must be at least %d characters.", name, min))
  if (nchar(x) > max) stop(sprintf("`%s` must be at most %d characters.", name, max))
}

assert_int <- function(x, name, min = -Inf, max = Inf) {
  if (is.null(x)) return(invisible(TRUE))
  if (!is.numeric(x) || length(x) != 1 || is.na(x) || x %% 1 != 0) stop(sprintf("`%s` must be an integer.", name))
  if (x < min || x > max) stop(sprintf("`%s` must be between %s and %s.", name, min, max))
}

# -----------------------
# Correct endpoints
# -----------------------

hecate_search_concepts_impl <- function(client,
                                        query,
                                        vocabulary_id = NULL,
                                        standard_concept = NULL,
                                        domain_id = NULL,
                                        concept_class_id = NULL,
                                        limit = 20) {
  assert_string(query, "query", min = 1, max = 500)
  assert_int(limit, "limit", min = 1, max = 150)

  req <- hecate_request(
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

  res <- hecate_perform(req)
  toJSON(res, auto_unbox = TRUE, pretty = TRUE)
}

hecate_get_concept_by_id_impl <- function(client, id) {
  assert_int(id, "id", min = 1)

  req <- hecate_request(client, path = paste0("concepts/", id))
  res <- hecate_perform(req)

  if (is.list(res) && !is.null(res$error)) {
    out <- res
  } else if (is.list(res) && length(res) >= 1 && is.list(res[[1]])) {
    out <- res[[1]]
  } else {
    out <- res
  }

  toJSON(out, auto_unbox = TRUE, pretty = TRUE)
}

hecate_get_concept_relationships_impl <- function(client, id) {
  assert_int(id, "id", min = 1)
  req <- hecate_request(client, path = paste0("concepts/", id, "/relationships"))
  res <- hecate_perform(req)
  toJSON(res, auto_unbox = TRUE, pretty = TRUE)
}

hecate_get_concept_phoebe_impl <- function(client, id) {
  assert_int(id, "id", min = 1)
  req <- hecate_request(client, path = paste0("concepts/", id, "/phoebe"))
  res <- hecate_perform(req)
  toJSON(res, auto_unbox = TRUE, pretty = TRUE)
}

hecate_expand_concept_hierarchy_impl <- function(client, id, childLevels = 5, parentLevels = 0) {
  assert_int(id, "id", min = 1)
  assert_int(childLevels, "childLevels", min = 0, max = 10)
  assert_int(parentLevels, "parentLevels", min = 0, max = 10)

  req <- hecate_request(
    client,
    path = paste0("concepts/", id, "/expand"),
    query = list(childlevels = childLevels, parentlevels = parentLevels)
  )

  res <- hecate_perform(req)

  out <- res
  if (is.list(res) && is.null(res$error) && !is.null(res$concepts) && length(res$concepts) >= 1) {
    children <- res$concepts[[1]]$children %||% list()
    out <- children
  }

  toJSON(out, auto_unbox = TRUE, pretty = TRUE)
}

hecate_get_concept_counts_impl <- function(concept_counts_df, concept_ids) {
  if (is.null(concept_counts_df)) {
    return(toJSON(list(
      error = "data_not_loaded",
      message = "Concept counts data is not available."
    ), auto_unbox = TRUE, pretty = TRUE))
  }

  concept_ids <- as.integer(concept_ids)

  if (any(is.na(concept_ids)) || any(concept_ids < 1)) {
    return(toJSON(list(
      error = "invalid_input",
      message = "All concept_ids must be positive integers."
    ), auto_unbox = TRUE, pretty = TRUE))
  }

  result <- concept_counts_df %>%
    dplyr::filter(concept_id %in% concept_ids) %>%
    dplyr::arrange(concept_id, data_source)

  if (nrow(result) == 0) {
    return(toJSON(list(
      message = "No records found for the specified concept IDs.",
      concept_ids = concept_ids,
      results = list()
    ), auto_unbox = TRUE, pretty = TRUE))
  }

  if (inherits(result$record_count, "integer64")) {
    record_counts_char <- as.character(result$record_count)
    record_counts_num <- suppressWarnings(as.numeric(record_counts_char))

    if (all(!is.na(record_counts_num)) &&
        all(record_counts_num == floor(record_counts_num)) &&
        max(record_counts_num, na.rm = TRUE) <= .Machine$integer.max &&
        min(record_counts_num, na.rm = TRUE) >= 0) {
      result$record_count <- as.integer(record_counts_num)
    } else {
      result$record_count <- as.numeric(record_counts_char)
    }
  } else if (is.numeric(result$record_count)) {
    if (all(result$record_count == floor(result$record_count), na.rm = TRUE) &&
        max(result$record_count, na.rm = TRUE) <= .Machine$integer.max &&
        min(result$record_count, na.rm = TRUE) >= 0) {
      result$record_count <- as.integer(result$record_count)
    }
  }

  result_list <- lapply(1:nrow(result), function(i) {
    row_data <- as.list(result[i, ])
    if (!is.null(row_data$record_count) && !is.na(row_data$record_count)) {
      if (is.numeric(row_data$record_count) && row_data$record_count == floor(row_data$record_count)) {
        row_data$record_count <- as.integer(row_data$record_count)
      }
    }
    row_data
  })

  output <- list(
    concept_ids = unique(concept_ids),
    total_records = nrow(result),
    results = result_list
  )

  old_scipen <- getOption("scipen")
  on.exit(options(scipen = old_scipen), add = TRUE)
  options(scipen = 999)

  toJSON(output, auto_unbox = TRUE, pretty = TRUE, digits = 22)
}

# -----------------------
# Vocabulary search (for AI assistant and interactive use)
# -----------------------

#' Search OMOP vocabulary concepts via Hecate API
#'
#' Returns matches for a query string. Useful for resolving unmapped units,
#' drugs, conditions, etc.
#'
#' @param query Search text (e.g. "gram", "iu", "mg", "milliliter")
#' @param vocabulary_id Optional filter (e.g. "UCUM" for units)
#' @param standard_concept Optional: "S", "C", or NULL for all
#' @param domain_id Optional filter (e.g. "Unit")
#' @param limit Max results (default 20)
#' @param client Optional hecate_client; uses default if NULL
#' @param as_json If TRUE, return raw JSON; otherwise return parsed list
#' @return Parsed list of concepts (or JSON string if as_json=TRUE)
#' @examples
#' vocabulary_search("gram")
#' vocabulary_search("iu", vocabulary_id = "UCUM", limit = 10)
#' vocabulary_search("milliliter", vocabulary_id = "UCUM", domain_id = "Unit")
vocabulary_search <- function(query,
                              vocabulary_id = NULL,
                              standard_concept = NULL,
                              domain_id = NULL,
                              concept_class_id = NULL,
                              limit = 20,
                              client = NULL,
                              as_json = FALSE) {
  client <- client %||% hecate_client()
  json_res <- hecate_search_concepts_impl(
    client,
    query = query,
    vocabulary_id = vocabulary_id,
    standard_concept = standard_concept,
    domain_id = domain_id,
    concept_class_id = concept_class_id,
    limit = limit
  )

  if (as_json) return(json_res)

  res <- fromJSON(json_res, simplifyVector = TRUE)

  if (!is.null(res$error)) {
    return(res)
  }

  # Hecate may return concepts in different shapes; normalize for display
  if (is.data.frame(res)) {
    out <- res
  } else if (is.list(res) && !is.null(res$concepts)) {
    out <- res$concepts
  } else if (is.list(res) && length(res) > 0) {
    out <- res
  } else {
    out <- res
  }

  out
}

#' Get a single concept by ID
vocabulary_get_concept <- function(concept_id, client = NULL, as_json = FALSE) {
  client <- client %||% hecate_client()
  json_res <- hecate_get_concept_by_id_impl(client, as.integer(concept_id))
  if (as_json) return(json_res)
  fromJSON(json_res, simplifyVector = TRUE)
}
