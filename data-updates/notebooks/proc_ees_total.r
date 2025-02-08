# Databricks notebook source
# DBTITLE 1,Load dependencies
source("utils.R")

install.packages(
  c(
    "sparklyr",
    "DBI",
    "dplyr",
    "testthat"
  ),
  repos = repo_url
)

library(sparklyr)
library(DBI)

library(dplyr)
library(testthat)

ga4_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_ga4_total"
ua_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_ua_total"
write_table_name <- "catalog_40_copper_statistics_services.analytics_app.ees_total"

sc <- spark_connect(method = "databricks")

# COMMAND ----------

# DBTITLE 1,Read in and check table integrity
ua_data <- sparklyr::sdf_sql(sc, paste("SELECT * FROM", ua_table_name)) %>% collect()
ga4_data <- sparklyr::sdf_sql(sc, paste("SELECT * FROM", ga4_table_name)) %>% collect()

dates <- create_dates(max(ga4_data$date))

test_that("Col names match", {
  expect_equal(names(ua_data), names(ga4_data))
})

alltime_data <- rbind(ga4_data, ua_data) |>
  dplyr::arrange(desc(date))

test_that("No duplicate rows", {
  expect_true(nrow(alltime_data) == nrow(dplyr::distinct(alltime_data)))
})

test_that("Data has no missing values", {
  expect_false(any(is.na(alltime_data)))

  expect_equal(
    setdiff(alltime_data$date, seq(as.Date(dates$all_time_date), max(dates$latest_date), by = "day")) |>
      length(),
    0
  )
})

# COMMAND ----------

# DBTITLE 1,Write out app data
updated_spark_df <- copy_to(sc, alltime_data, overwrite = TRUE)

# Write to temp table while we confirm we're good to overwrite data
spark_write_table(updated_spark_df, paste0(write_table_name, "_temp"), mode = "overwrite")

temp_table_data <- sparklyr::sdf_sql(sc, paste0("SELECT * FROM ", write_table_name, "_temp")) %>% collect()
previous_data <- sparklyr::sdf_sql(sc, paste0("SELECT * FROM ", write_table_name)) %>% collect()

test_that("Temp table data matches updated data", {
  expect_equal(temp_table_data, alltime_data)
})

# Replace the old table with the new one
dbExecute(sc, paste0("DROP TABLE IF EXISTS ", write_table_name))
dbExecute(sc, paste0("ALTER TABLE ", write_table_name, "_temp RENAME TO ", write_table_name))

print_changes_summary(temp_table_data, previous_data)
