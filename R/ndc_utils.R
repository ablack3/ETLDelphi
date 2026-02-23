# ndc_utils.R
# NDC (National Drug Code) detection, normalization, and smart lookup utilities.
# NDCs are 10-digit codes with 3 possible segment formats: 5-4-1, 5-3-2, 4-4-2.
# Source data often has formatting issues: missing hyphens, * for -, leading zeros
# stripped, extra characters. These utilities generate all plausible formatted
# variants and try each against the vocabulary.

#' Test whether a string looks like an NDC code
#'
#' Heuristic: after stripping hyphens, spaces, and asterisks, is it 9-11 digits?
#' Also matches patterns like "59630*70248" or "00944-2620-01".
#'
#' @param x Character string to test.
#' @return Logical.
#' @export
is_ndc_like <- function(x) {
  if (!is.character(x) || length(x) != 1 || is.na(x)) return(FALSE)
  # Strip hyphens, spaces, asterisks, dots
  digits <- gsub("[^0-9]", "", x)
  ndig <- nchar(digits)
  # NDC codes are 10 digits (sometimes 11 with leading zero, or 9 with a stripped zero)
  # Also require at least some non-alpha content (not a pure drug name)
  has_digits <- ndig >= 9 && ndig <= 11
  # Reject if mostly letters (drug names)
  non_digit_stripped <- gsub("[0-9*\\-. ]", "", x)
  mostly_digits <- nchar(non_digit_stripped) <= 2
  has_digits && mostly_digits
}

#' Generate all plausible NDC formatted variants from a raw string
#'
#' Given a raw NDC-like string (possibly with *, missing hyphens, etc.),
#' generates normalized variants to try against vocabulary:
#' \itemize{
#'   \item Digits-only (all non-digits stripped)
#'   \item 11-digit zero-padded
#'   \item All 3 hyphenated formats: 5-4-1, 5-3-2, 4-4-2
#'   \item Original with * replaced by 0
#' }
#'
#' @param x Raw NDC string.
#' @return Character vector of unique variants to try.
#' @export
ndc_variants <- function(x) {
  if (!is.character(x) || length(x) != 1 || is.na(x)) return(character(0))

  # Step 1: Replace * with 0 (common suppression character)
  star_replaced <- gsub("\\*", "0", x)

  # Step 2: Extract digits only
  digits <- gsub("[^0-9]", "", star_replaced)
  ndig <- nchar(digits)

  if (ndig < 9 || ndig > 11) return(unique(c(x, star_replaced)))

  variants <- character(0)

  # Raw digits
  variants <- c(variants, digits)

  # 11-digit padded
  if (ndig <= 11) {
    padded_11 <- sprintf("%011s", digits)
    padded_11 <- gsub(" ", "0", padded_11)
    variants <- c(variants, padded_11)
  }

  # Hyphenated formats (from 10 or 11 digit strings)
  # For 11 digits: strip leading 0 to get 10, then format
  d10 <- if (ndig == 11 && substr(digits, 1, 1) == "0") {
    substr(digits, 2, 11)
  } else if (ndig == 10) {
    digits
  } else if (ndig == 9) {
    # Pad to 10 with leading zero
    paste0("0", digits)
  } else {
    NULL
  }

  if (!is.null(d10) && nchar(d10) == 10) {
    # 5-4-1
    fmt_541 <- paste0(substr(d10, 1, 5), "-", substr(d10, 6, 9), "-", substr(d10, 10, 10))
    # 5-3-2
    fmt_532 <- paste0(substr(d10, 1, 5), "-", substr(d10, 6, 8), "-", substr(d10, 9, 10))
    # 4-4-2
    fmt_442 <- paste0(substr(d10, 1, 4), "-", substr(d10, 5, 8), "-", substr(d10, 9, 10))
    variants <- c(variants, fmt_541, fmt_532, fmt_442)

    # Also try without hyphens but with the star-replaced original format
    # e.g., if original was "58914*01310", try "5891401310"
    variants <- c(variants, d10)
  }

  # Also try 11-digit hyphenated
  if (!is.null(d10) && ndig <= 11) {
    d11 <- sprintf("%011s", digits)
    d11 <- gsub(" ", "0", d11)
    if (nchar(d11) == 11) {
      # 5-4-2 (FDA format with leading 0)
      fmt_542 <- paste0(substr(d11, 1, 5), "-", substr(d11, 6, 9), "-", substr(d11, 10, 11))
      variants <- c(variants, fmt_542, d11)
    }
  }

  # Include the original and star-replaced
  variants <- c(variants, x, star_replaced)

  unique(variants[nchar(variants) > 0])
}

#' Smart NDC lookup: try all normalized variants against OMOPHub
#'
#' Generates all plausible NDC variants from a raw string and tries each
#' against OMOPHub's by-code endpoint. Returns the first successful match
#' along with the standard (RxNorm) concept via "Maps to" relationship.
#'
#' @param raw_ndc Raw NDC string from source data.
#' @param oh_client An \code{omophub_client} object.
#' @param hc_client A \code{hecate_client} object (for relationship lookup fallback).
#' @return List with \code{ndc_concept} (the NDC concept found), \code{standard_concept}
#'   (the RxNorm standard concept it maps to), \code{matched_variant} (which format matched),
#'   or \code{list(found = FALSE)} if no variant matches.
#' @export
lookup_ndc_smart <- function(raw_ndc, oh_client = NULL, hc_client = NULL) {
  oh_client <- oh_client %||% omophub_client()
  variants <- ndc_variants(raw_ndc)

  ndc_concept <- NULL
  matched_variant <- NULL

  for (v in variants) {
    Sys.sleep(0.2)
    res <- tryCatch(
      omophub_get_by_code("NDC", v, client = oh_client),
      error = function(e) list(error = conditionMessage(e))
    )

    # Check if we got a valid concept back
    if (!is.null(res) && is.null(res$error) && !is.null(res$data)) {
      concept_data <- if (is.list(res$data) && !is.null(res$data$concept_id)) {
        res$data
      } else if (is.list(res$data) && length(res$data) > 0 && is.list(res$data[[1]])) {
        res$data[[1]]
      } else {
        NULL
      }
      if (!is.null(concept_data) && !is.null(concept_data$concept_id)) {
        ndc_concept <- concept_data
        matched_variant <- v
        break
      }
    }
  }

  if (is.null(ndc_concept)) {
    return(list(found = FALSE, variants_tried = length(variants)))
  }

  # Now find the standard concept via "Maps to"
  standard_concept <- NULL

  # Try OMOPHub mappings endpoint first
  map_res <- tryCatch(
    omophub_map_codes(
      source_codes = list(list(vocabulary_id = "NDC", concept_code = matched_variant)),
      target_vocabulary = "RxNorm",
      client = oh_client
    ),
    error = function(e) NULL
  )

  if (!is.null(map_res) && !is.null(map_res$data) && length(map_res$data) > 0) {
    mapping <- map_res$data[[1]]
    if (!is.null(mapping$target_concept) || !is.null(mapping$mappings)) {
      targets <- mapping$target_concept %||% mapping$mappings
      if (is.list(targets)) {
        target <- if (!is.null(targets$concept_id)) targets else if (length(targets) > 0) targets[[1]] else NULL
        if (!is.null(target) && !is.null(target$concept_id) && target$standard_concept == "S") {
          standard_concept <- target
        }
      }
    }
  }

  # Fallback: use Hecate relationships if OMOPHub mapping didn't work
  if (is.null(standard_concept) && !is.null(hc_client)) {
    rel_res <- tryCatch(
      hecate_get_relationships(ndc_concept$concept_id, client = hc_client),
      error = function(e) NULL
    )
    if (!is.null(rel_res) && is.list(rel_res)) {
      rels <- if (!is.null(rel_res$relationships)) rel_res$relationships else rel_res
      if (is.list(rels)) {
        for (r in rels) {
          if (!is.null(r$relationship_id) && r$relationship_id == "Maps to" &&
              !is.null(r$concept_id_2) && !is.null(r$standard_concept) && r$standard_concept == "S") {
            standard_concept <- list(
              concept_id = r$concept_id_2,
              concept_name = r$concept_name_2 %||% r$concept_name,
              vocabulary_id = r$vocabulary_id_2 %||% "RxNorm",
              standard_concept = "S"
            )
            break
          }
        }
      }
    }
  }

  list(
    found = TRUE,
    ndc_concept = ndc_concept,
    standard_concept = standard_concept,
    matched_variant = matched_variant,
    variants_tried = length(variants)
  )
}
