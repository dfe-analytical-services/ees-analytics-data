# Databricks notebook source
# DBTITLE 1,Install and load dependencies
source("utils.R")

packages <- c(
  "googleAnalyticsR",
  "googleAuthR",
  "sparklyr",
  "DBI",
  "dplyr",
  "tidyr",
  "testthat",
  "lubridate",
  "arrow"
)

install_if_needed(packages)
lapply(packages, library, character.only = TRUE)

table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_ga4_events"

sc <- spark_connect(method = "databricks")

# COMMAND ----------

# DBTITLE 1,Authenticate
ga_auth(json = auth_path)

# COMMAND ----------

# DBTITLE 1,Create table if not exists
dbExecute(sc, paste(
  "CREATE TABLE IF NOT EXISTS",
  table_name,
  "(date DATE, pagePath STRING, eventName STRING, eventLabel STRING, eventCategory STRING, eventCount DOUBLE)"
))

# COMMAND ----------

# DBTITLE 1,Check for latest date from existing data
last_date <- sparklyr::sdf_sql(sc, paste("SELECT MAX(date) FROM", table_name)) %>%
  collect() %>%
  pull() %>%
  as.character()

if (is.na(last_date)) {
  # Before tracking started so gets the whole series that is available
  last_date <- "2022-02-02"
}

reference_dates <- create_dates(Sys.Date() - 2) # doing this to make sure the data is complete when we request it

changes_since <- as.Date(last_date) + 1
changes_to <- as.Date(reference_dates$latest_date)

test_that("Query dates are valid", {
  expect_true(is.Date(changes_since))
  expect_true(grepl("\\d{4}-\\d{2}-\\d{2}", as.character(changes_since)))
  expect_true(is.Date(changes_to))
  expect_true(grepl("\\d{4}-\\d{2}-\\d{2}", as.character(changes_to)))

  if (changes_to < changes_since) {
    # Exit the notebook early
    dbutils.notebook.exit("Data is up to date, skipping the rest of the notebook")
  }
})

# COMMAND ----------

# DBTITLE 1,Get previous data
previous_data <- sparklyr::sdf_sql(sc, paste("SELECT * FROM", table_name))

# COMMAND ----------

# DBTITLE 1,Pull in data
latest_data <- ga_data(
  369420610,
  metrics = c("eventCount"),
  dimensions = c("date", "pagePath", "eventName", "customEvent:event_label", "customEvent:event_category"),
  date_range = c(changes_since, changes_to),
  limit = -1
) |>
  dplyr::rename("eventLabel" = "customEvent:event_label") |>
  dplyr::rename("eventCategory" = "customEvent:event_category")

# COMMAND ----------

# DBTITLE 1,Append new data onto old
test_that("Col names match", {
  expect_equal(names(latest_data), colnames(previous_data)) # colnames is the spark_df equivalent of names
})

latest_data <- copy_to(sc, latest_data, overwrite = TRUE)

updated_data <- rbind(previous_data, latest_data) |>
  dplyr::arrange(desc(date)) |>
  tidyr::drop_na()

# COMMAND ----------

# DBTITLE 1,Quick data integrity checks
test_that("New data has more rows than previous data", {
  expect_true(sdf_nrow(updated_data) > sdf_nrow(previous_data))
})

test_that("New data has no duplicate rows", {
  expect_true(sdf_nrow(updated_data) == sdf_nrow(sdf_distinct(updated_data)))
})

test_that("Latest date is as expected", {
  expect_equal( 
    updated_data %>%
      sdf_distinct("date") %>%
      sdf_read_column("date") %>%
      max(), 
    changes_to
  )
})

test_that("Data has no missing values", {
  expect_false(any(is.na(updated_data)))
})

test_that("There are no missing dates since we started GA4", {
  expect_equal(
    setdiff(
      updated_data %>% sdf_distinct("date") %>% sdf_read_column("date"), 
      seq(as.Date(reference_dates$ga4_date), changes_to, by = "day")
    ) |>
      length(),
    0
  )
})

# COMMAND ----------

# DBTITLE 1,Write to table
# Write to temp table while we confirm we're good to overwrite data
spark_write_table(updated_data, paste0(table_name, "_temp"), mode = "overwrite")

temp_table_data <- sparklyr::sdf_sql(sc, paste0("SELECT * FROM ", table_name, "_temp")) %>% collect()

test_that("Temp table data matches updated data", {
  expect_equal(nrow(temp_table_data), sdf_nrow(updated_data))
})

# Replace the old table with the new one
dbExecute(sc, paste0("DROP TABLE IF EXISTS ", table_name))
dbExecute(sc, paste0("ALTER TABLE ", table_name, "_temp RENAME TO ", table_name))

sdf_print_changes_summary(temp_table_data, previous_data)
