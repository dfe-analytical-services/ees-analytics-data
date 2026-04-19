# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Reads parquet files from the EES public API Volume and writes them incrementally to Delta tables in Unity Catalog.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC - **Cluster libraries**: `sparklyr` and `arrow` are pre-installed on DBR. `duckdb` and `DBI` are installed at runtime if missing.
# MAGIC - **Permissions**: Read access to the source Volume at `/Volumes/catalog_40_copper_statistics_services/statistics_services/mv_statistics_services/public-api/`. Write access to `catalog_40_copper_statistics_services.statistics_services`.
# MAGIC - **Filename format**: Source parquet files must be prefixed with `YYYYMMdd-HHmmss` (e.g., `20240315-143022-queries.parquet`). This prefix is parsed into `_file_datetime` and `_file_date` metadata columns.

# COMMAND ----------

# DBTITLE 1,Install and load dependencies
# -- Configuration -----------------------------------------------------------

TARGET_CATALOG <- "catalog_40_copper_statistics_services"
TARGET_SCHEMA  <- "statistics_services"

volume_path <- "/Volumes/catalog_40_copper_statistics_services/statistics_services/mv_statistics_services/public-api/"

# -- Dependencies ------------------------------------------------------------

options(repos = c(CRAN = "https://packagemanager.posit.co/cran/__linux__/noble/latest"))

if (!requireNamespace("duckdb", quietly = TRUE)) {
    install.packages("duckdb")
}
if (!requireNamespace("DBI", quietly = TRUE)) {
    install.packages("DBI")
}

library(sparklyr)
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
#' Uses a fast-path check: if the newest local file (by name) is already in the
#' table, all files are assumed processed and the function exits immediately.
#' This avoids expensive full-table scans when nothing has changed.
#'
#' New-file detection uses a Spark anti-join rather than collecting the full
#' processed-file list into R memory, so R-side memory usage scales with the
#' number of *new* files, not total files in the table.
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
    base_path = volume_path
) {
    full_table_name <- paste0(TARGET_CATALOG, ".", TARGET_SCHEMA, ".", table_name)

    # Build folder path, normalising any double slashes
    full_folder <- if (!is.null(base_path)) {
        gsub("//+", "/", paste0(base_path, "/", folder, "/"))
    } else {
        sub("/?$", "/", folder)
    }

    # List and optionally filter source files
    # NOTE: recursive = FALSE is used intentionally. The source folders are flat
    # (no subdirectories). Using recursive = TRUE causes R to stat() every file
    # through the FUSE mount, adding ~38ms per file — over 2 minutes for large
    # folders. recursive = FALSE avoids this and is ~200x faster.
    all_files <- paste0(full_folder, list.files(full_folder, recursive = FALSE))
    if (!is.null(pattern)) {
        all_files <- all_files[grepl(pattern, all_files)]
    }

    # -- QA: Source folder must contain files ----------------------------------
    if (length(all_files) == 0) {
        stop(sprintf("No source files found in %s (pattern: %s)",
                     full_folder, if (is.null(pattern)) "all" else pattern))
    }

    # Fast check: look up the newest local file in the table.
    # If it exists, all files are already processed (assumes append-only source
    # folder with chronologically-ordered filenames). Returns TRUE (found),
    # FALSE (not found), or NA (table doesn't exist).
    #
    # NOTE: SQL is built via sprintf because Spark SQL and DuckDB don't support
    # parameterised table/column names. Inputs are filesystem paths (not user-
    # supplied), so injection risk is minimal. Values are single-quote-escaped.
    newest_file <- sort(all_files, decreasing = TRUE)[1]
    newest_in_table <- tryCatch({
        result <- sdf_sql(sc, sprintf(
            "SELECT 1 FROM %s WHERE _source_file = '%s' LIMIT 1",
            full_table_name, gsub("'", "''", newest_file)
        )) %>% collect()
        nrow(result) > 0
    }, error = function(e) NA)

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
    # Identify new files via Spark anti-join — avoids collecting the full
    # processed-file list into R memory. R-side memory scales with new-file
    # count only, not the total number of files already in the table.
    if (is.na(newest_in_table)) {
        message("Table doesn't exist yet, will create it.")
        new_files <- all_files
        pre_file_count <- 0L
    } else {
        all_files_sdf <- copy_to(
            sc, data.frame(`_source_file` = all_files),
            name = paste0("temp_all_files_", table_name),
            overwrite = TRUE
        )
        existing_sdf <- sdf_sql(sc, sprintf(
            "SELECT DISTINCT _source_file FROM %s", full_table_name
        ))
        new_files <- anti_join(all_files_sdf, existing_sdf, by = "_source_file") %>%
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
    # These are parsed from the filename; NULLs indicate a filename format that
    # doesn't match the expected YYYYMMdd-HHmmss prefix pattern.
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
    # Guards against corrupt filenames producing nonsensical dates.
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
    # Both missing and extra columns are treated as errors. Missing columns
    # indicate a structural break in the source. Extra columns require explicit
    # schema evolution — silently dropping them risks data loss.
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

    # -- Write to Delta with error handling ------------------------------------
    # tryCatch ensures the function always returns an audit result, even if the
    # write fails. Without this, a failed write leaves audit_results unpopulated
    # for this table and the validation summary silently omits it.
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

        # Use "overwrite" for first-time table creation, "append" for incremental
        write_mode <- if (is.na(newest_in_table)) "overwrite" else "append"
        spark_write_table(
            temp_sdf,
            name = full_table_name,
            mode = write_mode
        )

        # -- QA: Post-write row count must match expected ---------------------
        # Delta writes are atomic, so a mismatch here indicates a serious
        # problem (e.g. concurrent modification, or silently dropped rows).
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
# MAGIC | Table | Source | Pattern |
# MAGIC | --- | --- | --- |
# MAGIC | `raw_ees_top_level` | top-level/ | all |
# MAGIC | `raw_ees_query_access` | queries/ | *query-access* |
# MAGIC | `raw_ees_queries` | queries/ | *queries.parquet |
# MAGIC | `raw_ees_data_sets` | data-sets/ | all |
# MAGIC | `raw_ees_publications` | publications/ | all |
# MAGIC | `raw_ees_data_set_versions` | data-set-versions/ | all |

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
    table_name = "raw_ees_top_level"
)

# COMMAND ----------

# DBTITLE 1,Write raw_ees_query_access
audit_results[["raw_ees_query_access"]] <- write_to_delta_incremental(
    folder = "queries",
    pattern = "query-access",
    table_name = "raw_ees_query_access"
)

# COMMAND ----------

# DBTITLE 1,Write raw_ees_queries
audit_results[["raw_ees_queries"]] <- write_to_delta_incremental(
    folder = "queries",
    pattern = "queries\\.parquet",
    table_name = "raw_ees_queries"
)

# COMMAND ----------

# DBTITLE 1,Write raw_ees_data_sets
audit_results[["raw_ees_data_sets"]] <- write_to_delta_incremental(
    folder = "data-sets",
    table_name = "raw_ees_data_sets"
)

# COMMAND ----------

# DBTITLE 1,Write raw_ees_publications
audit_results[["raw_ees_publications"]] <- write_to_delta_incremental(
    folder = "publications",
    table_name = "raw_ees_publications"
)

# COMMAND ----------

# DBTITLE 1,Write raw_ees_data_set_versions
audit_results[["raw_ees_data_set_versions"]] <- write_to_delta_incremental(
    folder = "data-set-versions",
    table_name = "raw_ees_data_set_versions"
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
