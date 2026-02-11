#!/usr/bin/env Rscript
# Build drug_name -> concept_id mapping via Hecate API for unmapped drugs.
# Run before ETL to improve drug mappings when NDC is missing.
#
# Usage:
#   Rscript extras/build_drug_name_mapping.R
#   Rscript extras/build_drug_name_mapping.R --limit 200 --input mapping_quality_results/02_top_unmapped_source_values.csv
#
# Output: inst/extdata/drug_name_to_concept.csv

library(dplyr)

# Parse args
args <- commandArgs(trailingOnly = TRUE)
limit <- 150L
input_file <- "mapping_quality_results/02_top_unmapped_source_values.csv"
output_file <- "inst/extdata/drug_name_to_concept.csv"

i <- 1
while (i <= length(args)) {
  if (args[i] == "--limit" && i < length(args)) {
    limit <- as.integer(args[i + 1])
    i <- i + 2
  } else if (args[i] == "--input" && i < length(args)) {
    input_file <- args[i + 1]
    i <- i + 2
  } else if (args[i] == "--output" && i < length(args)) {
    output_file <- args[i + 1]
    i <- i + 2
  } else {
    i <- i + 1
  }
}

source("extras/hecate_client.R")

# Get top unmapped drug names
if (!file.exists(input_file)) {
  stop("Input file not found: ", input_file, "\nRun analyze_mapping_quality() first.")
}

unmapped <- read.csv(input_file, stringsAsFactors = FALSE) %>%
  filter(domain == "drug") %>%
  arrange(desc(record_count)) %>%
  head(limit)

drug_names <- unique(trimws(unmapped$source_value))
drug_names <- drug_names[nzchar(drug_names)]
message("Looking up ", length(drug_names), " drug names via Hecate...")

# Pick best standard drug concept from Hecate response
# Accept RxNorm and RxNorm Extension; prefer Ingredient for ingredients
pick_best_concept <- function(concepts_df) {
  if (is.null(concepts_df) || !is.data.frame(concepts_df) || nrow(concepts_df) == 0) return(NA_integer_)
  rx <- concepts_df[
    concepts_df$vocabulary_id %in% c("RxNorm", "RxNorm Extension") &
    concepts_df$standard_concept == "S",
    , drop = FALSE
  ]
  if (nrow(rx) == 0) return(NA_integer_)
  # Prefer Ingredient > Clinical Drug > Branded Drug
  ing <- rx[rx$concept_class_id == "Ingredient", , drop = FALSE]
  if (nrow(ing) > 0) return(ing$concept_id[1])
  cd <- rx[rx$concept_class_id == "Clinical Drug", , drop = FALSE]
  if (nrow(cd) > 0) return(cd$concept_id[1])
  bd <- rx[rx$concept_class_id == "Branded Drug", , drop = FALSE]
  if (nrow(bd) > 0) return(bd$concept_id[1])
  return(rx$concept_id[1])
}

# Common drug name synonyms (source_value -> Hecate search query)
DRUG_SYNONYMS <- c(
  "5-FU" = "fluorouracil",
  "5-Fluorouracil" = "fluorouracil",
  "FLUOROURACIL" = "fluorouracil",
  "EPIVIR" = "lamivudine",
  "HEPSERA" = "adefovir",
  "NEXIUM" = "esomeprazole",
  "ACIPHEX" = "rabeprazole",
  "AVAPRO" = "irbesartan",
  "DILANTIN" = "phenytoin",
  "ZYPREXA" = "olanzapine",
  "LANTUS" = "insulin glargine",
  "LYRICA" = "pregabalin",
  "CYMBALTA" = "duloxetine",
  "HUMALOG" = "insulin lispro",
  "ACTOS" = "pioglitazone",
  "AVANDIA" = "rosiglitazone",
  "LEVAQUIN" = "levofloxacin",
  "XELODA" = "capecitabine",
  "ELOXATIN" = "oxaliplatin",
  "HYCAMTIN" = "topotecan",
  "DACOGEN" = "decitabine",
  "ARAVA" = "leflunomide",
  "SAVELLA" = "milnacipran",
  "INTRON" = "interferon alfa-2b",
  "ROFERON-A" = "interferon alfa-2a",
  "ULTRALYTIC" = "urokinase",
  "ATNATIV" = "antithrombin",
  "INNOHEP" = "tinzaparin",
  "ABBOKINASE" = "urokinase",
  "OMS" = "morphine sulfate"
)

results <- list()
for (i in seq_along(drug_names)) {
  drug <- drug_names[i]
  query <- DRUG_SYNONYMS[drug]
  if (is.na(query)) query <- drug
  Sys.sleep(0.3)  # Rate limit
  res <- tryCatch(
    vocabulary_search(query, domain_id = "Drug", limit = 1),
    error = function(e) {
      message("  Error for '", drug, "': ", conditionMessage(e))
      NULL
    }
  )
  if (is.null(res) || !is.data.frame(res) || nrow(res) == 0) {
    results[[drug]] <- NA_integer_
    next
  }
  if (!is.null(res$error)) {
    results[[drug]] <- NA_integer_
    next
  }
  concepts <- res$concepts[[1]]
  cid <- pick_best_concept(concepts)
  results[[drug]] <- cid
  if (i %% 20 == 0) message("  Progress: ", i, "/", length(drug_names))
}

# Build output
out <- data.frame(
  drug_name = names(results),
  concept_id = unlist(results, use.names = FALSE),
  stringsAsFactors = FALSE
) %>%
  filter(!is.na(concept_id), concept_id > 0)

# Ensure output dir exists
dir.create(dirname(output_file), recursive = TRUE, showWarnings = FALSE)
write.csv(out, output_file, row.names = FALSE)
message("Wrote ", nrow(out), " mappings to ", output_file)
