# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Reads parquet files from the EES public API Volume and writes them incrementally to Delta tables in Unity Catalog.
# MAGIC
# MAGIC Supports **dual-source file discovery**: files are collected from both the structured subfolder path (`public-api/<folder>/`) and the Volume root. This handles the upstream Data Factory pipeline writing files to either location.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC - **Cluster libraries**: `sparklyr` and `arrow` are pre-installed on DBR. `duckdb` and `DBI` are installed at runtime if missing.
# MAGIC - **Permissions**: Read access to the source Volume at `/Volumes/catalog_40_copper_statistics_services/statistics_services/mv_statistics_services/`. Write access to `catalog_40_copper_statistics_services.statistics_services`.
# MAGIC - **Filename format**: Source parquet files must be prefixed with `YYYYMMdd-HHmmss` (e.g., `20240315-143022-queries.parquet`). This prefix is parsed into `_file_datetime` and `_file_date` metadata columns.

# COMMAND ----------

# DBTITLE 1,Install and load dependencies
# -- Configuration -----------------------------------------------------------

TARGET_CATALOG <- "catalog_40_copper_statistics_services"
TARGET_SCHEMA  <- "statistics_services"

# Volume root and structured subfolder path. The upstream Data Factory pipeline
# originally wrote files into public-api/<subfolder>/, but since 2026-02-13 has
# been writing them flat to the Volume root instead. The ingestion function
# searches both locations so it works regardless of where files land.
volume_root <- "/Volumes/catalog_40_copper_statistics_services/statistics_services/mv_statistics_services/"
volume_path <- paste0(volume_root, "public-api/")

# -- Dependencies ------------------------------------------------------------

options(repos = c(CRAN = "https://packagemanager.posit.co/cran/__linux__/noble/latest"))

if (!requireNamespace("duckdb", quietly = TRUE)) {
    install.packages("duckdb")
}
if (!requireNamespace("DBI", quietly = TRUE)) {
    install.packages("DBI")
}

library(sparklyr)
library(dplyr)
library(duckdb)
library(DBI)
library(arrow)
sc <- spark_connect(method = "databricks")

# COMMAND ----------

# DBTITLE 1,Incremental Delta write function
#' Write new parquet files incrementally to a Delta table
#'
#' Reads parquet files from a Volume folder using DuckDB, identifies which files
#' have already been processed (tracked via the `_source_file` column), and
#' appends only new data to the target Delta table. Creates the table on first
#' run.
#'
#' Supports dual-source file discovery: files are collected from both the
#' structured subfolder (`base_path/folder/`) and the Volume root (filtered by
#' `root_pattern`). This handles the case where an upstream pipeline changes
#' its output path without notice.
#'
#' Deduplication is by **basename** (filename without path), so the same
#' parquet file at two different paths is never ingested twice. When a file
#' exists in both the subfolder and the root, the subfolder version is kept
#' for backward compatibility with existing `_source_file` references.
#'
#' Uses a fast-path check: if the newest local file (by basename) is already
#' in the table, all files are assumed processed and the function exits
#' immediately. This avoids expensive full-table scans when nothing has changed.
#'
#' New-file detection uses a Spark anti-join on basename rather than full path,
#' so files are correctly recognised as already-processed even if the source
#' path has changed.
#'
#' Each row is enriched with metadata columns extracted from the source filename:
#' - `_source_file`: full file path (used for incremental tracking)
#' - `_file_datetime`: timestamp parsed from the filename prefix (YYYYMMdd-HHmmss)
#' - `_file_date`: date parsed from the filename prefix (YYYYMMdd)
#'
#' Inline QA checks (all fail-fast via `stop()`):
#' - No source files found in Volume folder
#' - New parquet files contain zero rows
#' - NULLs in metadata columns (_source_file, _file_datetime, _file_date)
#' - Parsed file dates outside plausible range (before 2020 or in the future)
#' - Schema mismatch: existing table columns missing from new data, or new
#'   columns not present in the existing table
#' - Post-write row count does not match expected delta
#'
#' @param folder Character. Subfolder name relative to the base Volume path
#'   (e.g., "top-level", "queries"). Leading/trailing slashes are normalised.
#' @param pattern Character or NULL. Regex pattern passed to `grepl()` to
#'   filter filenames. Partial matches are supported. Default: NULL (all files).
#' @param table_name Character. Target table name (without catalog/schema prefix).
#' @param root_pattern Character or NULL. Regex pattern to match files at the
#'   Volume root level. When provided, files in `volume_root` matching this
#'   pattern are combined with files from the subfolder. Default: NULL (subfolder
#'   only).
#' @param base_path Character or NULL. Base Volume path. Defaults to
#'   `volume_path`. Set to NULL to treat `folder` as an absolute path.
#'
#' @return Invisibly returns a named list with audit metadata:
#'   - `table_name`, `full_table_name`: identifiers
#'   - `status`: "success", "skipped", or "error"
#'   - `source_file_count`: number of matching files in the Volume
#'   - `files_new`, `files_total`: files processed this run / cumulative
#'   - `rows_written`, `rows_total`: rows appended / total in table
#'   - `latest_file_date`: most recent _file_date in the new data
#'   - `error_message`: (error status only) the error description
write_to_delta_incremental <- function(
    folder,
    pattern = NULL,
    table_name,
    root_pattern = NULL,
    base_path = volume_path
) {
    full_table_name <- paste0(TARGET_CATALOG, ".", TARGET_SCHEMA, ".", table_name)

    # Build folder path, normalising any double slashes
    full_folder <- if (!is.null(base_path)) {
        gsub("//+", "/", paste0(base_path, "/", folder, "/"))
    } else {
        sub("/?$", "/", folder)
    }

    # -- Collect files from subfolder -----------------------------------------
    # NOTE: recursive = FALSE is used intentionally. The source folders are flat
    # (no subdirectories). Using recursive = TRUE causes R to stat() every file
    # through the FUSE mount, adding ~38ms per file - over 2 minutes for large
    # folders. recursive = FALSE avoids this and is ~200x faster.
    subfolder_files <- if (dir.exists(full_folder)) {
        raw <- list.files(full_folder, recursive = FALSE)
        full_paths <- paste0(full_folder, raw)
        if (!is.null(pattern)) full_paths[grepl(pattern, full_paths)] else full_paths
    } else {
        character(0)
    }

    # -- Collect files from Volume root (dual-source) -------------------------
    # Since 2026-02-13 the upstream Data Factory pipeline writes files flat to
    # the Volume root instead of into public-api/<subfolder>/. When root_pattern
    # is provided, we also scan volume_root and include matching files.
    root_files <- if (!is.null(root_pattern)) {
        raw <- list.files(volume_root, recursive = FALSE)
        full_paths <- paste0(volume_root, raw[grepl(root_pattern, raw)])
        # Only include .parquet files to avoid matching directories
        full_paths[grepl("\\.parquet$", full_paths)]
    } else {
        character(0)
    }

    # -- Combine and deduplicate by basename ----------------------------------
    # Subfolder files are listed first so that if the same filename appears at
    # both paths, !duplicated() keeps the subfolder version. This preserves
    # backward compatibility with _source_file values already in the table.
    combined <- c(subfolder_files, root_files)
    all_files <- combined[!duplicated(basename(combined))]

    if (length(subfolder_files) > 0 && length(root_files) > 0) {
        n_deduped <- length(combined) - length(all_files)
        message(sprintf(
            "Found files in subfolder (%d) + Volume root (%d) for %s%s",
            length(subfolder_files), length(root_files), table_name,
            if (n_deduped > 0) sprintf(" (%d duplicates removed)", n_deduped) else ""
        ))
    }

    # -- QA: At least one source must contain files ----------------------------
    if (length(all_files) == 0) {
        stop(sprintf("No source files found in %s or Volume root (pattern: %s / %s)",
                     full_folder,
                     if (is.null(pattern)) "all" else pattern,
                     if (is.null(root_pattern)) "none" else root_pattern))
    }

    # Fast check: look up the newest local file in the table by basename.
    # IMPORTANT: sort by basename, not full path. Full-path sorting breaks when
    # files come from different directories (e.g. "public-api/queries/2026..." 
    # sorts after "20260419..." because 'p' > '2', giving wrong "newest").
    # Returns TRUE (found), FALSE (not found), or NA (table doesn't exist).
    #
    # NOTE: Spark SQL regexp_extract requires 3 args: (str, regexp, groupIdx).
    newest_file <- all_files[order(basename(all_files), decreasing = TRUE)[1]]
    newest_basename <- basename(newest_file)
    newest_in_table <- tryCatch({
        result <- sdf_sql(sc, sprintf(
            "SELECT 1 FROM %s WHERE regexp_extract(_source_file, '([^/]+)$', 1) = '%s' LIMIT 1",
            full_table_name, gsub("'", "''", newest_basename)
        )) %>% collect()
        nrow(result) > 0
    }, error = function(e) {
        msg <- conditionMessage(e)
        # Only return NA (= table doesn't exist) for genuine table-not-found
        # errors. All other errors are re-raised so they aren't silently
        # swallowed (e.g. SQL syntax errors, permission issues).
        if (grepl("TABLE_OR_VIEW_NOT_FOUND|Table or view not found|AnalysisException", msg)) {
            NA
        } else {
            stop(e)
        }
    })

    if (isTRUE(newest_in_table)) {
        message(
            "No new files to process for ", table_name,
            " (", length(all_files), " files, newest already processed)"
        )
        return(invisible(list(
            table_name        = table_name,
            full_table_name   = full_table_name,
            status            = "skipped",
            source_file_count = length(all_files)
        )))
    }

    # Table doesn't exist (NA) or has unprocessed files (FALSE).
    # Identify new files via Spark anti-join on basename rather than full path.
    # This ensures a file is recognised as already-processed even if it was
    # ingested from a different path (e.g. subfolder vs root migration).
    if (is.na(newest_in_table)) {
        message("Table doesn't exist yet, will create it.")
        new_files <- all_files
        pre_file_count <- 0L
    } else {
        all_files_df <- data.frame(
            `_source_file` = all_files,
            `_basename` = basename(all_files),
            stringsAsFactors = FALSE,
            check.names = FALSE
        )
        all_files_sdf <- copy_to(
            sc, all_files_df,
            name = paste0("temp_all_files_", table_name),
            overwrite = TRUE
        )
        existing_sdf <- sdf_sql(sc, sprintf(
            "SELECT DISTINCT regexp_extract(_source_file, '([^/]+)$', 1) AS _basename FROM %s",
            full_table_name
        ))
        new_files <- anti_join(all_files_sdf, existing_sdf, by = "_basename") %>%
            collect() %>% `[[`("_source_file")
        pre_file_count <- sdf_sql(sc, sprintf(
            "SELECT COUNT(DISTINCT _source_file) AS cnt FROM %s", full_table_name
        )) %>% collect() %>% `[[`("cnt")
    }

    if (length(new_files) == 0) {
        message("No new files to process for ", table_name)
        return(invisible(list(
            table_name        = table_name,
            full_table_name   = full_table_name,
            status            = "skipped",
            source_file_count = length(all_files)
        )))
    }

    message(
        "Processing ", length(new_files), " new files (",
        pre_file_count, " already processed)..."
    )

    # Read new parquet files with DuckDB
    file_list <- paste0("[", paste0("'", new_files, "'", collapse = ", "), "]")

    con <- DBI::dbConnect(duckdb::duckdb())
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

    # NOTE: DuckDB regexp_extract uses 2 args (no group index needed)
    query <- sprintf(
        "SELECT
            *,
            filename AS _source_file,
            strptime(
                substr(regexp_extract(filename, '[^/]+$'), 1, 15),
                '%%Y%%m%%d-%%H%%M%%S'
            ) AS _file_datetime,
            strptime(
                substr(regexp_extract(filename, '[^/]+$'), 1, 8),
                '%%Y%%m%%d'
            )::DATE AS _file_date
        FROM read_parquet(%s, filename = true, union_by_name = true)",
        file_list
    )

    new_data <- DBI::dbGetQuery(con, query)

    # -- QA: New files must contain data --------------------------------------
    if (nrow(new_data) == 0) {
        stop(sprintf(
            "New parquet files contained zero rows for %s (%d files)",
            table_name, length(new_files)
        ))
    }

    # -- QA: Metadata columns must not contain NULLs --------------------------
    null_source   <- sum(is.na(new_data[["_source_file"]]))
    null_datetime <- sum(is.na(new_data[["_file_datetime"]]))
    null_date     <- sum(is.na(new_data[["_file_date"]]))

    if (null_source + null_datetime + null_date > 0) {
        stop(sprintf(
            paste0(
                "Metadata NULL check failed for %s (%d rows): ",
                "_source_file=%d, _file_datetime=%d, _file_date=%d NULLs"
            ),
            table_name, nrow(new_data), null_source, null_datetime, null_date
        ))
    }

    # -- QA: Parsed file dates must be within a plausible range ----------------
    file_dates <- as.Date(new_data[["_file_date"]])

    if (any(file_dates > Sys.Date() + 1)) {
        stop(sprintf(
            "Future dates detected in %s: max _file_date is %s (today is %s)",
            table_name, max(file_dates), Sys.Date()
        ))
    }
    if (any(file_dates < as.Date("2020-01-01"))) {
        stop(sprintf(
            "Implausibly old dates in %s: min _file_date is %s",
            table_name, min(file_dates)
        ))
    }

    # -- QA: Schema compatibility with existing table -------------------------
    if (!is.na(newest_in_table)) {
        existing_cols <- tryCatch({
            colnames(sdf_sql(sc, sprintf("SELECT * FROM %s LIMIT 0", full_table_name)))
        }, error = function(e) character(0))

        if (length(existing_cols) > 0) {
            new_cols <- names(new_data)
            missing_cols <- setdiff(existing_cols, new_cols)
            extra_cols <- setdiff(new_cols, existing_cols)

            if (length(missing_cols) > 0) {
                stop(sprintf(
                    "Schema mismatch for %s: existing columns missing from new data: %s",
                    table_name, paste(missing_cols, collapse = ", ")
                ))
            }
            if (length(extra_cols) > 0) {
                stop(sprintf(
                    "Schema mismatch for %s: new columns not in existing table: %s. Resolve via schema evolution or manual ALTER TABLE.",
                    table_name, paste(extra_cols, collapse = ", ")
                ))
            }
        }
    }

    message("Writing ", nrow(new_data), " rows to ", full_table_name, "...")

    write_result <- tryCatch({
        pre_write_count <- if (!is.na(newest_in_table)) {
            sdf_sql(sc, sprintf(
                "SELECT COUNT(*) AS cnt FROM %s", full_table_name
            )) %>% collect() %>% `[[`("cnt")
        } else {
            0L
        }

        temp_sdf <- copy_to(
            sc, new_data,
            name = paste0("temp_", table_name),
            overwrite = TRUE,
            serializer = "arrow"
        )

        write_mode <- if (is.na(newest_in_table)) "overwrite" else "append"
        spark_write_table(
            temp_sdf,
            name = full_table_name,
            mode = write_mode
        )

        post_write_count <- sdf_sql(sc, sprintf(
            "SELECT COUNT(*) AS cnt FROM %s", full_table_name
        )) %>% collect() %>% `[[`("cnt")

        expected_count <- pre_write_count + nrow(new_data)
        if (post_write_count != expected_count) {
            stop(sprintf(
                "Post-write row count mismatch for %s: expected %d (pre=%d + new=%d), found %d",
                table_name, expected_count, pre_write_count, nrow(new_data), post_write_count
            ))
        }

        total_files <- pre_file_count + length(new_files)
        latest_file_date <- max(file_dates)

        message(sprintf(
            "Done! %s: %d rows written, %d total from %d files (latest: %s). Row count verified.",
            table_name, nrow(new_data), post_write_count, total_files, latest_file_date
        ))

        list(
            table_name        = table_name,
            full_table_name   = full_table_name,
            status            = "success",
            files_new         = length(new_files),
            files_total       = total_files,
            rows_written      = nrow(new_data),
            rows_total        = post_write_count,
            latest_file_date  = latest_file_date,
            source_file_count = length(all_files)
        )
    }, error = function(e) {
        message(sprintf("ERROR writing %s: %s", table_name, conditionMessage(e)))
        list(
            table_name        = table_name,
            full_table_name   = full_table_name,
            status            = "error",
            error_message     = conditionMessage(e),
            source_file_count = length(all_files)
        )
    })

    invisible(write_result)
}

# COMMAND ----------

# DBTITLE 1,Write to Delta section
# MAGIC %md
# MAGIC ## Write to Delta Tables (Incremental)
# MAGIC
# MAGIC The cells below write each dataset to Delta tables in `catalog_40_copper_statistics_services.statistics_services`.
# MAGIC
# MAGIC On first run, all files are processed. On subsequent runs, only new files are appended.
# MAGIC
# MAGIC Each table searches both its structured subfolder and the Volume root for matching files.
# MAGIC
# MAGIC | Table | Subfolder | Subfolder pattern | Root pattern |
# MAGIC | --- | --- | --- | --- |
# MAGIC | `raw_ees_top_level` | top-level/ | all | public-api-top-level-calls |
# MAGIC | `raw_ees_query_access` | queries/ | \*query-access\* | public-api-query-access |
# MAGIC | `raw_ees_queries` | queries/ | \*queries.parquet | public-api-queries.parquet |
# MAGIC | `raw_ees_data_sets` | data-sets/ | all | public-api-data-set-calls |
# MAGIC | `raw_ees_publications` | publications/ | all | public-api-publications-calls |
# MAGIC | `raw_ees_data_set_versions` | data-set-versions/ | all | public-api-data-set-version-calls |

# COMMAND ----------

# DBTITLE 1,Initialise audit collector
# Collect structured results from each table write for the final validation
# summary. Each call to write_to_delta_incremental() returns a named list with
# status, file counts, row counts, and latest file date.
audit_results <- list()

# COMMAND ----------

# DBTITLE 1,Write raw_ees_top_level
audit_results[["raw_ees_top_level"]] <- write_to_delta_incremental(
    folder = "top-level",
    table_name = "raw_ees_top_level",
    root_pattern = "public-api-top-level-calls"
)

# COMMAND ----------

# DBTITLE 1,Write raw_ees_query_access
audit_results[["raw_ees_query_access"]] <- write_to_delta_incremental(
    folder = "queries",
    pattern = "query-access",
    table_name = "raw_ees_query_access",
    root_pattern = "public-api-query-access"
)

# COMMAND ----------

# DBTITLE 1,Write raw_ees_queries
audit_results[["raw_ees_queries"]] <- write_to_delta_incremental(
    folder = "queries",
    pattern = "queries\\.parquet",
    table_name = "raw_ees_queries",
    root_pattern = "public-api-queries\\.parquet"
)

# COMMAND ----------

# DBTITLE 1,Write raw_ees_data_sets
audit_results[["raw_ees_data_sets"]] <- write_to_delta_incremental(
    folder = "data-sets",
    table_name = "raw_ees_data_sets",
    root_pattern = "public-api-data-set-calls"
)

# COMMAND ----------

# DBTITLE 1,Write raw_ees_publications
audit_results[["raw_ees_publications"]] <- write_to_delta_incremental(
    folder = "publications",
    table_name = "raw_ees_publications",
    root_pattern = "public-api-publications-calls"
)

# COMMAND ----------

# DBTITLE 1,Write raw_ees_data_set_versions
audit_results[["raw_ees_data_set_versions"]] <- write_to_delta_incremental(
    folder = "data-set-versions",
    table_name = "raw_ees_data_set_versions",
    root_pattern = "public-api-data-set-version-calls"
)

# COMMAND ----------

# DBTITLE 1,Post-ingestion validation
# MAGIC %md
# MAGIC ## Post-Ingestion Validation
# MAGIC
# MAGIC Checks run after all tables are written:
# MAGIC
# MAGIC 1. **Audit summary** — displays a table of per-table status, file counts, row counts, and latest file dates
# MAGIC 2. **Data freshness** — warns if the latest `_file_date` in any table is older than the threshold (default: 5 days)
# MAGIC 3. **Source file count** — warns if the Delta table references more files than currently exist in the Volume (indicates source files were deleted)
# MAGIC 4. **Date completeness** — for each table, generates the expected daily date sequence from min to max `_file_date` and reports any missing days, grouped into contiguous ranges

# COMMAND ----------

# DBTITLE 1,Run post-ingestion validation
# -- Build audit summary from collected results --------------------------------
safe_get <- function(x, field, default = NA) {
    val <- x[[field]]
    if (is.null(val)) default else val
}

# For skipped runs, latest_file_date won't be in the audit result — query the
# actual table so the summary always shows the real current state.
get_table_latest_date <- function(full_table_name) {
    tryCatch({
        sdf_sql(sc, sprintf(
            "SELECT MAX(_file_date) AS max_date FROM %s", full_table_name
        )) %>% collect() %>% `[[`("max_date") %>% as.character()
    }, error = function(e) NA_character_)
}

summary_df <- do.call(rbind, lapply(audit_results, function(r) {
    # Use the run's latest_file_date if available, otherwise look it up
    latest <- safe_get(r, "latest_file_date")
    latest <- if (is.null(latest) || is.na(latest)) {
        get_table_latest_date(r$full_table_name)
    } else {
        as.character(latest)
    }

    data.frame(
        table            = safe_get(r, "table_name"),
        status           = safe_get(r, "status"),
        files_new        = safe_get(r, "files_new", NA_integer_),
        files_total      = safe_get(r, "files_total", NA_integer_),
        source_files     = safe_get(r, "source_file_count", NA_integer_),
        rows_written     = safe_get(r, "rows_written", NA_integer_),
        rows_total       = safe_get(r, "rows_total", NA_integer_),
        latest_file_date = latest,
        stringsAsFactors = FALSE
    )
}))

cat("\n=== Ingestion Audit Summary ===", "\n")
cat("Run at:", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"), "\n\n")
print(summary_df, row.names = FALSE)

# -- Data freshness: latest file date should be within threshold ---------------
# Uses the dates already looked up for the summary above.
FRESHNESS_THRESHOLD_DAYS <- 5
freshness_cutoff <- Sys.Date() - FRESHNESS_THRESHOLD_DAYS
stale_tables <- character(0)

for (i in seq_len(nrow(summary_df))) {
    latest <- summary_df$latest_file_date[i]
    if (!is.na(latest) && as.Date(latest) < freshness_cutoff) {
        stale_tables <- c(stale_tables, sprintf(
            "  %s (latest: %s)", summary_df$table[i], latest
        ))
    }
}

if (length(stale_tables) > 0) {
    warning(sprintf(
        "STALE DATA (threshold: %d days, cutoff: %s):\n%s",
        FRESHNESS_THRESHOLD_DAYS, freshness_cutoff,
        paste(stale_tables, collapse = "\n")
    ))
}

# -- Source vs table file count: detect deleted Volume files -------------------
# If the table references more distinct _source_file values than currently exist
# in the Volume, files have been removed from the source since they were ingested.
file_count_issues <- character(0)

for (r in audit_results) {
    if (!is.null(r$source_file_count) && !is.null(r$full_table_name)) {
        table_file_count <- tryCatch({
            sdf_sql(sc, sprintf(
                "SELECT COUNT(DISTINCT _source_file) AS cnt FROM %s",
                r$full_table_name
            )) %>% collect() %>% `[[`("cnt")
        }, error = function(e) NA)

        if (!is.na(table_file_count) && table_file_count > r$source_file_count) {
            file_count_issues <- c(file_count_issues, sprintf(
                "  %s: %d files in table vs %d in Volume",
                r$table_name, table_file_count, r$source_file_count
            ))
        }
    }
}

if (length(file_count_issues) > 0) {
    warning(sprintf(
        "SOURCE FILE COUNT MISMATCH (files may have been deleted from Volume):\n%s",
        paste(file_count_issues, collapse = "\n")
    ))
}

cat("\n=== Validation complete ===", "\n")

# COMMAND ----------

# DBTITLE 1,Date completeness section
# MAGIC %md
# MAGIC ## Date Completeness Check
# MAGIC
# MAGIC For each table, this queries the full range of `_file_date` values, generates the expected daily sequence, and highlights any missing days. Gaps may indicate failed pipeline runs or missing source files.
# MAGIC
# MAGIC A summary table shows coverage stats (including weekend vs weekday gap counts), followed by detailed gap listings. Each contiguous range is tagged `[weekend]` (all Sat/Sun — likely expected) or `[WEEKDAY]` (includes at least one Mon–Fri — likely a pipeline issue).

# COMMAND ----------

# DBTITLE 1,Check date completeness across all tables
# -- Date completeness check across all tables --------------------------------
# For each table, identify the date range and any missing days.
# Gaps are classified as weekend-only (Sat/Sun) or weekday to help distinguish
# expected non-run days from genuine pipeline failures.

table_names <- c(
    "raw_ees_top_level",
    "raw_ees_query_access",
    "raw_ees_queries",
    "raw_ees_data_sets",
    "raw_ees_publications",
    "raw_ees_data_set_versions"
)

full_table_prefix <- paste0(TARGET_CATALOG, ".", TARGET_SCHEMA, ".")

# Helper: classify a vector of dates as all-weekend or includes-weekday
is_weekend <- function(d) weekdays(d) %in% c("Saturday", "Sunday")

# Collect date coverage for each table
completeness <- lapply(table_names, function(tbl) {
    full_name <- paste0(full_table_prefix, tbl)
    tryCatch({
        dates_df <- sdf_sql(sc, sprintf(
            "SELECT DISTINCT _file_date FROM %s ORDER BY _file_date", full_name
        )) %>% collect()

        actual_dates <- as.Date(dates_df[["_file_date"]])

        if (length(actual_dates) == 0) {
            return(list(
                summary = data.frame(
                    table = tbl, min_date = NA, max_date = NA,
                    total_days_span = NA, days_with_data = 0,
                    days_missing = NA, weekend_gaps = NA, weekday_gaps = NA,
                    completeness_pct = NA,
                    stringsAsFactors = FALSE
                ),
                missing_dates = as.Date(character(0))
            ))
        }

        min_d <- min(actual_dates)
        max_d <- max(actual_dates)
        expected <- seq.Date(min_d, max_d, by = "day")
        missing  <- expected[!expected %in% actual_dates]

        list(
            summary = data.frame(
                table            = tbl,
                min_date         = as.character(min_d),
                max_date         = as.character(max_d),
                total_days_span  = length(expected),
                days_with_data   = length(actual_dates),
                days_missing     = length(missing),
                weekend_gaps     = sum(is_weekend(missing)),
                weekday_gaps     = sum(!is_weekend(missing)),
                completeness_pct = round(length(actual_dates) / length(expected) * 100, 1),
                stringsAsFactors = FALSE
            ),
            missing_dates = missing
        )
    }, error = function(e) {
        list(
            summary = data.frame(
                table = tbl, min_date = NA, max_date = NA,
                total_days_span = NA, days_with_data = NA,
                days_missing = NA, weekend_gaps = NA, weekday_gaps = NA,
                completeness_pct = NA,
                stringsAsFactors = FALSE
            ),
            missing_dates = as.Date(character(0))
        )
    })
})

# -- Summary table -------------------------------------------------------------
summary_tbl <- do.call(rbind, lapply(completeness, `[[`, "summary"))

cat("\n=== Date Completeness Summary ===\n")
cat("Generated:", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"), "\n\n")
print(summary_tbl, row.names = FALSE)

# -- Detailed gap report per table ---------------------------------------------
# Each contiguous range of missing dates is tagged:
#   [weekend]  — all days in the range fall on Sat/Sun
#   [WEEKDAY]  — at least one weekday is missing (needs attention)
tables_with_gaps <- Filter(
    function(x) length(x$missing_dates) > 0,
    completeness
)

if (length(tables_with_gaps) == 0) {
    cat("\nAll tables have complete daily coverage across their date range.\n")
} else {
    cat(sprintf("\n%d table(s) have missing dates:\n", length(tables_with_gaps)))
    for (item in tables_with_gaps) {
        tbl_name <- item$summary$table
        gaps     <- item$missing_dates

        n_weekend <- sum(is_weekend(gaps))
        n_weekday <- length(gaps) - n_weekend

        cat(sprintf(
            "\n  %s \u2014 %d missing day(s): %d weekend, %d weekday\n",
            tbl_name, length(gaps), n_weekend, n_weekday
        ))

        # Group consecutive missing dates into ranges
        gaps_sorted <- sort(gaps)
        breaks <- c(0, which(diff(gaps_sorted) > 1), length(gaps_sorted))

        for (i in seq_len(length(breaks) - 1)) {
            range_dates <- gaps_sorted[(breaks[i] + 1):breaks[i + 1]]
            start <- range_dates[1]
            end   <- range_dates[length(range_dates)]
            tag   <- if (all(is_weekend(range_dates))) "[weekend]" else "[WEEKDAY]"

            range_str <- if (start == end) {
                sprintf("%s (%s)", start, weekdays(start))
            } else {
                sprintf("%s to %s (%dd)", start, end, as.integer(end - start) + 1L)
            }

            cat(sprintf("     %s %s\n", tag, range_str))
        }
    }
}

cat("\n=== Date completeness check complete ===\n")

# COMMAND ----------

# DBTITLE 1,Potential issues to investigate
# MAGIC %md
# MAGIC ### Potential issues to investigate
# MAGIC
# MAGIC Based on the date completeness check above, the following patterns may warrant further investigation.
# MAGIC
# MAGIC #### 1. Data Factory migration gap (2026-02-13 to 2026-02-16) — all tables
# MAGIC
# MAGIC Every table is missing dates around 2026-02-13, coinciding with the upstream Data Factory pipeline switching its output path from `public-api/<subfolder>/` to the Volume root. This is almost certainly a known outage rather than a bug, but worth confirming:
# MAGIC
# MAGIC * Were source files ever generated for these dates and subsequently lost?
# MAGIC * Can the missing days be backfilled from another source or ADF run history?
# MAGIC
# MAGIC #### 2. Sustained early-period gaps in `raw_ees_query_access` and `raw_ees_queries` (Mar–Aug 2025)
# MAGIC
# MAGIC Both tables share an identical pattern of frequent weekday gaps from late March through early August 2025 (20–22 weekday gaps each), including a continuous 7-day gap from 2025-06-06 to 2025-06-12. This suggests the upstream pipeline for the `/queries/` endpoint was unreliable during its early months. Consider:
# MAGIC
# MAGIC * Was the API or Data Factory pipeline still being commissioned during this period?
# MAGIC * Are the missing days recoverable, or should the effective start date for these tables be treated as \~August 2025 for analytics purposes?
# MAGIC * Do downstream consumers of these tables already account for this sparse early coverage?
# MAGIC
# MAGIC #### 3. Scattered isolated weekday gaps in `raw_ees_data_sets` (Aug–Oct 2025)
# MAGIC
# MAGIC Unlike the query tables (which have clustered early gaps), `raw_ees_data_sets` has isolated single-day weekday misses on 2025-08-06 (Wed), 2025-08-14 (Thu), and 2025-10-29 (Wed). These look like individual pipeline run failures rather than a systemic issue:
# MAGIC
# MAGIC * Check ADF run history for these specific dates to confirm whether the pipeline failed or simply didn't trigger.
# MAGIC * If the API was available on those days, a one-off backfill may be straightforward.
# MAGIC
# MAGIC #### 4. Weekend gaps are pervasive but may be expected
# MAGIC
# MAGIC The majority of missing days across all tables fall on weekends (e.g. 13 of 16 gaps in `raw_ees_publications`, 7 of 9 in `raw_ees_top_level`). If the pipeline is not scheduled to run on weekends, these are expected and can be safely excluded from completeness calculations. To confirm:
# MAGIC
# MAGIC * Is the Data Factory pipeline configured with a weekday-only schedule?
# MAGIC * If so, consider filtering weekends out of the completeness percentage to give a more meaningful metric (currently `raw_ees_publications` shows 94.8% but would be \~98.6% on a weekday-only basis).
# MAGIC
# MAGIC #### 5. Single unexplained weekday gaps in `raw_ees_top_level` and `raw_ees_publications`
# MAGIC
# MAGIC `raw_ees_top_level` is missing 2025-07-17 (Thursday) and `raw_ees_publications` is missing 2025-07-17 (Thursday) and 2025-08-05 (Tuesday). These are shared with no other table, suggesting endpoint-specific issues rather than a whole-pipeline failure. Low priority, but worth a quick check if these dates matter for reporting.

# COMMAND ----------

# DBTITLE 1,Backfill helper section
# MAGIC %md
# MAGIC ## Backfill Helper
# MAGIC
# MAGIC This helper inspects the missing dates identified by the completeness check and classifies each gap as **recoverable** (source files exist in the Volume but weren't ingested) or **unrecoverable** (no source files found — needs upstream re-delivery).
# MAGIC
# MAGIC ### Configuration
# MAGIC
# MAGIC | Parameter | Default | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `BACKFILL_EXCLUDE_RANGES` | 2026-02-13 to 2026-02-16 | Date ranges to skip (e.g. known migration windows) |
# MAGIC | `BACKFILL_DRY_RUN` | `TRUE` | When `TRUE`, only reports findings. Set to `FALSE` to actually re-ingest recoverable files |
# MAGIC
# MAGIC The helper reuses the `completeness` list produced by the date completeness check cell above, so that cell must be run first.

# COMMAND ----------

# DBTITLE 1,Backfill helper for missing dates
# -- Backfill helper -----------------------------------------------------------
# Scans the Volume for source files matching missing dates and classifies each
# gap as recoverable or unrecoverable.
#
# Depends on: `completeness` list from the date completeness check cell.

# -- Configuration -------------------------------------------------------------
BACKFILL_DRY_RUN <- TRUE   # Set FALSE to actually re-ingest recoverable files

# Known outage windows to exclude from the backfill scan
BACKFILL_EXCLUDE_RANGES <- list(
    list(from = as.Date("2026-02-13"), to = as.Date("2026-02-16"),
         reason = "Data Factory migration window")
)

# Mapping from table name to the subfolder / patterns used by the ingestion
# function, so we can look up files and optionally re-trigger ingestion.
table_config <- list(
    raw_ees_top_level = list(
        folder = "top-level", pattern = NULL,
        root_pattern = "public-api-top-level-calls"
    ),
    raw_ees_query_access = list(
        folder = "queries", pattern = "query-access",
        root_pattern = "public-api-query-access"
    ),
    raw_ees_queries = list(
        folder = "queries", pattern = "queries\\.parquet",
        root_pattern = "public-api-queries\\.parquet"
    ),
    raw_ees_data_sets = list(
        folder = "data-sets", pattern = NULL,
        root_pattern = "public-api-data-set-calls"
    ),
    raw_ees_publications = list(
        folder = "publications", pattern = NULL,
        root_pattern = "public-api-publications-calls"
    ),
    raw_ees_data_set_versions = list(
        folder = "data-set-versions", pattern = NULL,
        root_pattern = "public-api-data-set-version-calls"
    )
)

in_exclude_range <- function(d) {
    dominated <- rep(FALSE, length(d))
    for (rng in BACKFILL_EXCLUDE_RANGES) {
        dominated <- dominated | (d >= rng$from & d <= rng$to)
    }
    dominated
}

# -- Scan each table -----------------------------------------------------------
cat("\n=== Backfill Helper ===")
cat(sprintf("\nMode: %s", if (BACKFILL_DRY_RUN) "DRY RUN (report only)" else "LIVE (will re-ingest)"))
cat(sprintf("\nExclude ranges: %d defined\n",
    length(BACKFILL_EXCLUDE_RANGES)))

backfill_summary <- list()

for (item in completeness) {
    tbl <- item$summary$table
    missing <- item$missing_dates
    if (length(missing) == 0) next

    # Apply exclusion filters
    excluded <- in_exclude_range(missing)
    excluded_dates <- missing[excluded]
    missing <- missing[!excluded]

    if (length(missing) == 0) {
        cat(sprintf("\n  %s — no actionable gaps (all filtered out)\n", tbl))
        backfill_summary[[tbl]] <- list(
            actionable = 0, recoverable = 0, unrecoverable = 0,
            excluded = length(excluded_dates)
        )
        next
    }

    cfg <- table_config[[tbl]]
    if (is.null(cfg)) {
        cat(sprintf("\n  %s — SKIPPED (no config found)\n", tbl))
        next
    }

    # Collect all source files for this table (same logic as ingestion function)
    subfolder <- gsub("//+", "/", paste0(volume_path, "/", cfg$folder, "/"))
    subfolder_files <- if (dir.exists(subfolder)) {
        raw <- list.files(subfolder, recursive = FALSE)
        fps <- paste0(subfolder, raw)
        if (!is.null(cfg$pattern)) fps[grepl(cfg$pattern, fps)] else fps
    } else {
        character(0)
    }

    root_files <- if (!is.null(cfg$root_pattern)) {
        raw <- list.files(volume_root, recursive = FALSE)
        fps <- paste0(volume_root, raw[grepl(cfg$root_pattern, raw)])
        fps[grepl("\\.parquet$", fps)]
    } else {
        character(0)
    }

    combined <- c(subfolder_files, root_files)
    all_files <- combined[!duplicated(basename(combined))]

    # Extract date prefix (YYYYMMdd) from each filename
    file_dates <- tryCatch(
        as.Date(substr(basename(all_files), 1, 8), format = "%Y%m%d"),
        error = function(e) rep(NA, length(all_files))
    )

    # Match missing dates to available files
    recoverable_dates  <- missing[missing %in% file_dates]
    unrecoverable_dates <- missing[!missing %in% file_dates]

    recoverable_files <- if (length(recoverable_dates) > 0) {
        all_files[file_dates %in% recoverable_dates]
    } else {
        character(0)
    }

    backfill_summary[[tbl]] <- list(
        actionable    = length(missing),
        recoverable   = length(recoverable_dates),
        unrecoverable = length(unrecoverable_dates),
        excluded      = length(excluded_dates),
        recoverable_files = recoverable_files,
        recoverable_dates = recoverable_dates,
        unrecoverable_dates = unrecoverable_dates
    )

    cat(sprintf(
        "\n  %s — %d actionable gap(s): %d recoverable, %d unrecoverable, %d excluded\n",
        tbl, length(missing), length(recoverable_dates),
        length(unrecoverable_dates), length(excluded_dates)
    ))

    if (length(recoverable_dates) > 0) {
        cat(sprintf("     Recoverable dates: %s\n",
            paste(sort(recoverable_dates), collapse = ", ")))
        cat(sprintf("     Source files found: %d\n", length(recoverable_files)))
    }

    if (length(unrecoverable_dates) > 0) {
        cat(sprintf("     Unrecoverable dates (no source files): %s\n",
            paste(sort(unrecoverable_dates), collapse = ", ")))
    }
}

# -- Summary table -------------------------------------------------------------
cat("\n--- Backfill Summary ---\n")
summary_rows <- do.call(rbind, lapply(names(backfill_summary), function(tbl) {
    s <- backfill_summary[[tbl]]
    data.frame(
        table         = tbl,
        actionable    = s$actionable,
        recoverable   = s$recoverable,
        unrecoverable = s$unrecoverable,
        excluded      = s$excluded,
        stringsAsFactors = FALSE
    )
}))
print(summary_rows, row.names = FALSE)

# -- Upstream recovery request list --------------------------------------------
all_unrecoverable <- do.call(rbind, lapply(names(backfill_summary), function(tbl) {
    s <- backfill_summary[[tbl]]
    if (length(s$unrecoverable_dates) > 0) {
        data.frame(
            table = tbl,
            missing_date = as.character(sort(s$unrecoverable_dates)),
            day_of_week  = weekdays(sort(s$unrecoverable_dates)),
            stringsAsFactors = FALSE
        )
    } else {
        NULL
    }
}))

if (!is.null(all_unrecoverable) && nrow(all_unrecoverable) > 0) {
    cat(sprintf(
        "\n--- Upstream Recovery Request (%d dates across %d tables) ---\n",
        nrow(all_unrecoverable),
        length(unique(all_unrecoverable$table))
    ))
    cat("Share this list with the Data Factory / API team:\n\n")
    print(all_unrecoverable, row.names = FALSE)
} else {
    cat("\nNo upstream recovery needed — all gaps are either excluded or recoverable.\n")
}

# -- Re-ingest recoverable files (when not in dry-run mode) --------------------
tables_with_recoverable <- Filter(
    function(tbl) {
        s <- backfill_summary[[tbl]]
        !is.null(s$recoverable) && s$recoverable > 0
    },
    names(backfill_summary)
)

if (length(tables_with_recoverable) > 0 && !BACKFILL_DRY_RUN) {
    cat("\n--- Re-ingesting recoverable files ---\n")
    for (tbl in tables_with_recoverable) {
        cfg <- table_config[[tbl]]
        cat(sprintf("\n  Re-ingesting %s...\n", tbl))
        tryCatch({
            result <- write_to_delta_incremental(
                folder       = cfg$folder,
                pattern      = cfg$pattern,
                table_name   = tbl,
                root_pattern = cfg$root_pattern
            )
            cat(sprintf("    Result: %s\n", result$status))
        }, error = function(e) {
            cat(sprintf("    ERROR: %s\n", conditionMessage(e)))
        })
    }
} else if (length(tables_with_recoverable) > 0 && BACKFILL_DRY_RUN) {
    cat(sprintf(
        "\n--- %d table(s) have recoverable files. Set BACKFILL_DRY_RUN <- FALSE to re-ingest. ---\n",
        length(tables_with_recoverable)
    ))
}

cat("\n=== Backfill helper complete ===\n")
