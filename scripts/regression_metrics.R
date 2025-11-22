#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    install.packages("jsonlite", repos = "https://cloud.r-project.org")
  }
})

library(jsonlite)

read_required_table <- function(path) {
  if (!file.exists(path)) {
    stop(sprintf("Required file not found: %s", path))
  }
  read.delim(path, header = TRUE, sep = "\t", check.names = FALSE, na.strings = c("", "NA"))
}

trailing_numeric_columns <- function(df) {
  numeric_flags <- vapply(df, is.numeric, logical(1))
  if (!any(numeric_flags)) {
    return(integer())
  }
  last_non_numeric <- max(which(!numeric_flags))
  if (!is.finite(last_non_numeric)) {
    return(seq_len(ncol(df)))
  }
  quant_start <- last_non_numeric + 1
  if (quant_start > ncol(df)) {
    return(integer())
  }
  seq(quant_start, ncol(df))
}

compute_wide_metrics <- function(df) {
  quant_cols <- trailing_numeric_columns(df)
  runs <- length(quant_cols)
  if (runs == 0) {
    return(list(
      runs = 0,
      complete_rows = 0,
      data_completeness = NA_real_,
      non_missing_values = 0
    ))
  }
  quant_data <- df[, quant_cols, drop = FALSE]
  complete_rows <- sum(complete.cases(quant_data))
  non_missing_values <- sum(!is.na(as.matrix(quant_data)))
  total_cells <- nrow(df) * runs
  list(
    runs = runs,
    complete_rows = complete_rows,
    data_completeness = if (total_cells > 0) non_missing_values / total_cells else NA_real_,
    non_missing_values = non_missing_values
  )
}

compute_dataset_metrics <- function(dataset_dir, dataset_name) {
  precursors_long <- read_required_table(file.path(dataset_dir, "precursors_long.tsv"))
  precursors_wide <- read_required_table(file.path(dataset_dir, "precursors_wide.tsv"))
  protein_groups_long <- read_required_table(file.path(dataset_dir, "protein_groups_long.tsv"))
  protein_groups_wide <- read_required_table(file.path(dataset_dir, "protein_groups_wide.tsv"))

  precursor_wide_metrics <- compute_wide_metrics(precursors_wide)
  protein_wide_metrics <- compute_wide_metrics(protein_groups_wide)

  list(
    dataset = dataset_name,
    precursors = list(
      total = nrow(precursors_long),
      unique = nrow(precursors_wide),
      runs = precursor_wide_metrics$runs,
      complete_rows = precursor_wide_metrics$complete_rows,
      non_missing_values = precursor_wide_metrics$non_missing_values,
      data_completeness = precursor_wide_metrics$data_completeness
    ),
    protein_groups = list(
      total = nrow(protein_groups_long),
      unique = nrow(protein_groups_wide),
      runs = protein_wide_metrics$runs,
      complete_rows = protein_wide_metrics$complete_rows,
      non_missing_values = protein_wide_metrics$non_missing_values,
      data_completeness = protein_wide_metrics$data_completeness
    )
  )
}

results_dir <- commandArgs(trailingOnly = TRUE)
if (length(results_dir) == 0) {
  results_dir <- file.path(getwd(), "results")
} else {
  results_dir <- results_dir[[1]]
}

if (!dir.exists(results_dir)) {
  stop(sprintf("Results directory does not exist: %s", results_dir))
}

dataset_dirs <- list.dirs(results_dir, recursive = FALSE, full.names = TRUE)
if (length(dataset_dirs) == 0) {
  stop(sprintf("No dataset directories found in %s", results_dir))
}

for (dataset_dir in dataset_dirs) {
  dataset_name <- basename(dataset_dir)
  message(sprintf("Processing dataset: %s", dataset_name))
  metrics <- compute_dataset_metrics(dataset_dir, dataset_name)
  output_path <- file.path(dataset_dir, sprintf("metrics_%s.json", dataset_name))
  write_json(metrics, output_path, pretty = TRUE, auto_unbox = TRUE)
}