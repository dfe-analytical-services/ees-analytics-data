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
)

latest_data <- data.frame()

for (day in seq(changes_since, changes_to, by = "day")) {
  day <- as.Date(day)

  message("Querying for ", day)

    day_query_result <- search_analytics(
      siteURL = "https://explore-education-statistics.service.gov.uk/",
      startDate = day,
      endDate = day,
      dimensions = c("date", "query", "page"),
      searchType = "web",
      rowLimit = 200000, # seems odd as the actual limit is 25k but is what was in the example docs!
      walk_data = "byBatch"
    )

    if(is.null(nrow(day_query_result))){
      message("No rows found for ", day)
    } else {
      latest_data <- rbind(
        latest_data,
        day_query_result |> rename("pagePath" = page)
      )
    }

  message("Current rows: ", nrow(latest_data))
}

# COMMAND ----------

if(nrow(latest_data) == 0){
  dbutils.notebook.exit("No new rows were found therefore there's no need to update this table, skipping the rest of the notebook")
}

# COMMAND ----------

test_that("Col names match", {
  expect_equal(names(latest_data), colnames(previous_data)) # colnames is the spark_df equivalent of names
})

latest_data <- copy_to(sc, latest_data, overwrite = TRUE)

updated_data <- rbind(previous_data, latest_data) |>
  dplyr::arrange(desc(date))

# COMMAND ----------

# DBTITLE 1,Quick data integrity checks
test_that("New data has more rows than previous data", {
  expect_true(sdf_nrow(updated_data) > sdf_nrow(previous_data))
})

test_that("New data has no duplicate rows", {
  expect_true(sdf_nrow(updated_data) == sdf_nrow(sdf_distinct(updated_data)))
})

test_that("Latest date is as expected", {
  expect_equal(  updated_data %>%
  sdf_distinct("date") %>%
  sdf_read_column("date") %>%
  max(), 
  changes_to)
})

test_that("Data has no missing values", {
  expect_false(any(is.na(updated_data)))
})

test_that("There are no missing dates since we started GA4", {
  expect_equal(
    setdiff(updated_data %>% sdf_distinct("date") %>% sdf_read_column("date"), seq(as.Date(reference_dates$search_console_date), changes_to, by = "day")) |>
      length(),
    0
  )
})

# COMMAND ----------

# MAGIC %md
# MAGIC The sections below all used to be one code block, they have been broken out line by line to help with debugging an intermittent issue
# MAGIC - Issue should be fixed now, to merge back into one block if this works

# COMMAND ----------

# Write to temp table while we confirm we're good to overwrite data
spark_write_table(updated_data, paste0(table_name, "_temp"), mode = "overwrite")

# COMMAND ----------

temp_table_data <- sparklyr::sdf_sql(sc, paste0("SELECT * FROM ", table_name, "_temp"))

# COMMAND ----------

test_that("Temp table data matches updated data", {
 expect_equal(sdf_nrow(temp_table_data), sdf_nrow(updated_data))
})

# COMMAND ----------

# Replace the old table with the new one
dbExecute(sc, paste0("DROP TABLE IF EXISTS ", table_name))

# COMMAND ----------

dbExecute(sc, paste0("ALTER TABLE ", table_name, "_temp RENAME TO ", table_name))

# COMMAND ----------

sdf_print_changes_summary(temp_table_data, previous_data)
