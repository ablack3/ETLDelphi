# ETLDelphi

ETL from the **Delphi 100k** (Delphi/Delfi) source schema to **OMOP CDM v5.4** in DuckDB. The R package runs a sequenced set of SQL scripts that transform source tables into standard OMOP tables.

## Why use it?

The **Delphi/Delfi** data model represents enrollment, encounters, providers, and clinical events (problems, medication orders and fulfillment, immunizations, lab orders and results, vitals, allergies, therapy). That structure is well-suited for testing and development: you get a single, well-defined schema that maps cleanly into the OMOP CDM. ETLDelphi is intended for anyone who has (or is considering) Delphi 100k–style data and wants to load it into an OMOP CDM—e.g. for analytics, cohort building, or tooling that expects OMOP.

## Installation

Install from GitHub (requires the `remotes` package):

```r
remotes::install_github("OHDSI/ETLDelphi")
```

For working with OMOP CDM data in R, [CDMConnector](https://cran.r-project.org/package=CDMConnector) is commonly used:

```r
install.packages("CDMConnector")
```

## Running the ETL

Prerequisites (DuckDB with `src` and `cdm` schemas, source data loaded) and step-by-step instructions are in the vignettes:

- **How to run**: `vignette("how_to_run", package = "ETLDelphi")`
- **ETL workflow and design**: `vignette("etl", package = "ETLDelphi")`

## License and use in code

ETLDelphi is licensed under the **Apache License 2.0**. You may use, modify, and distribute it under the terms of that license. You are welcome to use this package in tests, examples, or other code; attribution is appreciated but not required beyond the license terms. See [LICENSE.md](LICENSE.md) for the full text.
