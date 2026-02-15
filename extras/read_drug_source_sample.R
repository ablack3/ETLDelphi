# =============================================================================
# read_drug_source_sample.R — Read first 100 rows of drug source data, write to file
# =============================================================================
# Run from project root or set paths. Reads medication_orders.csv (primary drug
# source) from DELPHI_SOURCE_DIR and writes first 100 rows to drug_source_first_100.csv.
# =============================================================================

delphi_source_dir <- Sys.getenv("DELPHI_SOURCE_DIR", "delphi100k")
delphi_source_dir <- normalizePath(delphi_source_dir, winslash = "/", mustWork = FALSE)

if (!dir.exists(delphi_source_dir)) {
  stop("Delphi source directory not found: ", delphi_source_dir,
       ". Set DELPHI_SOURCE_DIR or place data in delphi100k/.")
}

csv_path <- file.path(delphi_source_dir, "medication_orders.csv")
if (!file.exists(csv_path)) {
  stop("Drug source file not found: ", csv_path)
}

# Read first 100 rows (no need to load full file)
drug_sample <- utils::read.csv(csv_path, nrows = 100, check.names = FALSE)

# Remove provider identifiers and unique IDs from output (privacy)
cols_to_drop <- c("Member_ID", "Order_ID", "Order_Provider_ID", "Encounter_ID")
existing_drop <- intersect(cols_to_drop, names(drug_sample))
if (length(existing_drop) > 0L) {
  drug_sample <- drug_sample[, setdiff(names(drug_sample), existing_drop), drop = FALSE]
}

out_path <- "drug_source_first_100.csv"
write.csv(drug_sample, out_path, row.names = FALSE)
message("Wrote first ", nrow(drug_sample), " rows to ", normalizePath(out_path, mustWork = FALSE))
