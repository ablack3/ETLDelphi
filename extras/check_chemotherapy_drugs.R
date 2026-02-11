#!/usr/bin/env Rscript
library(CDMConnector)
library(dplyr)

con <- DBI::dbConnect(duckdb::duckdb(), "~/Desktop/delphi.duckdb")
cdm <- cdmFromCon(con, "cdm", "main")

# Known antineoplastic/chemotherapy ancestor concepts in OMOP:
# 35807372 - Antineoplastic agents (RxNorm)
# 43526465 - Antineoplastic and immunosuppressive agents (ATC L01)
# 41247252 - Antineoplastic agents (may vary by vocab)
ancestor_ids <- c(35807372L, 43526465L, 41247252L)

# First, find chemotherapy-related concepts in the vocabulary
cat("=== Searching concept table for chemotherapy/antineoplastic ===\n")
chemo_concepts <- cdm$concept %>%
  filter(
    grepl("antineoplastic|chemotherapy|cytotoxic|antineoplast", concept_name, ignore.case = TRUE) |
    concept_id %in% ancestor_ids
  ) %>%
  filter(standard_concept == "S" | concept_id %in% ancestor_ids) %>%
  select(concept_id, concept_name, vocabulary_id, standard_concept) %>%
  collect()
print(chemo_concepts)
cat("\n")

# Get total drug_exposure stats
cat("=== drug_exposure overview ===\n")
drug_stats <- cdm$drug_exposure %>%
  summarise(
    total = n(),
    mapped = sum(drug_concept_id > 0, na.rm = TRUE),
    unmapped = sum(drug_concept_id == 0, na.rm = TRUE)
  ) %>%
  collect()
print(drug_stats)
cat("\n")

# Try each ancestor - get descendants and count in drug_exposure
for (aid in ancestor_ids) {
  anc_name <- cdm$concept %>% filter(concept_id == aid) %>% pull(concept_name) %>% coalesce("(unknown)")
  cat("=== Ancestor", aid, ":", anc_name, "===\n")
  
  descendants <- cdm$concept_ancestor %>%
    filter(ancestor_concept_id == aid) %>%
    select(descendant_concept_id) %>%
    collect()
  
  if (nrow(descendants) == 0) {
    cat("  No descendants found (concept may not exist in vocab)\n\n")
    next
  }
  
  cat("  Descendants:", nrow(descendants), "\n")
  
  counts <- cdm$drug_exposure %>%
    inner_join(
      descendants,
      by = c("drug_concept_id" = "descendant_concept_id"),
      copy = TRUE
    ) %>%
    left_join(
      cdm$concept %>% select(concept_id, concept_name),
      by = c("drug_concept_id" = "concept_id")
    ) %>%
    count(drug_concept_id, concept_name, sort = TRUE) %>%
    collect()
  
  if (nrow(counts) == 0) {
    cat("  No drug_exposure records for these concepts\n\n")
    next
  }
  
  cat("  Records:", sum(counts$n), "\n")
  cat("  Top drugs:\n")
  print(head(counts, 15))
  cat("\n")
}

# Also check drug_source_value for common chemo drug names (for unmapped rows)
cat("=== Sample drug_source_value (unmapped, potential chemo keywords) ===\n")
unmapped_chemo <- cdm$drug_exposure %>%
  filter(drug_concept_id == 0, drug_source_value != "") %>%
  collect() %>%
  filter(grepl("methotrexate|fluorouracil|docetaxel|paclitaxel|carboplatin|cisplatin|doxorubicin|cyclophosphamide|vincristine|tamoxifen|capecitabine|oxaliplatin|irinotecan|gemcitabine|etoposide|cytarabine|bleomycin|trastuzumab|bevacizumab|imatinib|chemotherapy|chemo", 
               drug_source_value, ignore.case = TRUE))
if (nrow(unmapped_chemo) > 0) {
  print(unmapped_chemo %>% count(drug_source_value, sort = TRUE) %>% head(20))
} else {
  cat("  None found with common chemo drug names in source\n")
}

DBI::dbDisconnect(con, shutdown = TRUE)
cat("\nDone.\n")
