# Databricks notebook source
source("utils.R")

packages <- c(
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

# Handling search console separately as from GitHub
remotes::install_github("MarkEdmondson1234/searchConsoleR")
library(searchConsoleR)

table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_search_console"

sc <- spark_connect(method = "databricks")

# COMMAND ----------

# DBTITLE 1,Authenticate
scr_auth(json = auth_path)

# COMMAND ----------

# DBTITLE 1,Check for latest date from existing data
dbExecute(sc, paste(
  "CREATE TABLE IF NOT EXISTS",
  table_name,
  "(date DATE, pagePath STRING, query STRING, clicks DOUBLE,",
  "impressions DOUBLE, position DOUBLE)"
))

last_date <- sparklyr::sdf_sql(sc, paste("SELECT MAX(date) FROM", table_name)) %>%
  collect() %>%
  pull() %>%
  as.character()

if (is.na(last_date)) {
  # Before tracking started so gets the whole series that is available
  last_date <- "2023-10-07"
}

reference_dates <- create_dates(Sys.Date() - 3) # 3) # we only get Search Console data 3 days later

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

# MAGIC %md
# MAGIC Ideally I'd have walked through the data byDate, as that [returns more data](https://code.markedmondson.me/searchConsoleR/), but I couldn't get it to return anything when I tried to set it up.
# MAGIC
# MAGIC I'm suspicious of how much data it returns, as I can query the full date range in one function call, but it always says there is a max of 9 * 25k queries, so to maximise the available data I've set it up to query as much as it'll allow in one function call for each day required.

# COMMAND ----------

previous_data <- sparklyr::sdf_sql(
  sc,
  paste("SELECT * FROM", table_name)
) |>
  collect()

latest_data <- data.frame()

for (day in seq(changes_since, changes_to, by = "day")) {
  day <- as.Date(day)

  message("Querying for ", day)

  latest_data <- rbind(
    latest_data,
    search_analytics(
      siteURL = "https://explore-education-statistics.service.gov.uk/",
      startDate = day,
      endDate = day,
      dimensions = c("date", "query", "page"),
      searchType = "web",
      rowLimit = 200000, # seems odd as the actual limit is 25k but is what was in the example docs!
      walk_data = "byBatch"
    ) |>
      rename("pagePath" = page)
  )

  message("Current rows: ", nrow(latest_data))
}

# COMMAND ----------

test_that("Col names match", {
  expect_equal(names(latest_data), names(previous_data))
})

updated_data <- rbind(previous_data, latest_data) |>
  dplyr::arrange(desc(date))

# COMMAND ----------

# DBTITLE 1,Quick data integrity checks
test_that("New data has more rows than previous data", {
  expect_true(nrow(updated_data) > nrow(previous_data))
})

test_that("New data has no duplicate rows", {
  expect_true(nrow(updated_data) == nrow(dplyr::distinct(updated_data)))
})

test_that("Latest date is as expected", {
  expect_equal(updated_data$date[1], changes_to)
})

test_that("Data has no missing values", {
  expect_false(any(is.na(updated_data)))
})

test_that("There are no missing dates since we started GA4", {
  expect_equal(
    setdiff(updated_data$date, seq(as.Date(reference_dates$search_console_date), changes_to, by = "day")) |>
      length(),
    0
  )
})

# COMMAND ----------

ga4_spark_df <- copy_to(sc, updated_data, overwrite = TRUE)

# Write to temp table while we confirm we're good to overwrite data
spark_write_table(ga4_spark_df, paste0(table_name, "_temp"), mode = "overwrite")

temp_table_data <- sparklyr::sdf_sql(sc, paste0("SELECT * FROM ", table_name, "_temp"))

test_that("Temp table data matches updated data", {
 expect_equal(sdf_nrow(temp_table_data), nrow(updated_data)) # No collect()
})

# Replace the old table with the new one
dbExecute(sc, paste0("DROP TABLE IF EXISTS ", table_name))
dbExecute(sc, paste0("ALTER TABLE ", table_name, "_temp RENAME TO ", table_name))

print_changes_summary(temp_table_data, previous_data)
