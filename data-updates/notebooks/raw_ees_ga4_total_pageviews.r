# Databricks notebook source
# DBTITLE 1,Install and load dependencies
install.packages("googleAnalyticsR")
install.packages("googleAuthR")

library(googleAnalyticsR)
library(googleAuthR)
library(SparkR)
library(dplyr)
library(testthat)

table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_ga4_total_pageviews"

# COMMAND ----------

# DBTITLE 1,Authenticate
ga_auth(json = "/Volumes/catalog_40_copper_statistics_services/cam_testing/test-volume/ees-analytics-c5875719e665.json")


# COMMAND ----------

# DBTITLE 1,Check for latest date from existing data

# Initialize a Spark session
sparkR.session()

last_date <- sql(paste("SELECT MAX(date) FROM", table_name))

if (is.null(last_date)) {
  last_date <- "2022-02-02" # before tracking started so gets the whole series that is available
}

update_date <- paste0(Sys.Date() - 3)

# COMMAND ----------

# DBTITLE 1,Pull in data
previous_data <- sql(paste("SELECT * FROM", table_name))

latest_data <- ga_data(
  369420610,
  metrics = c("screenPageViews", "sessions"),
  dimensions = c("date"),
  date_range = c(last_date, update_date),
  limit = -1
)

updated_data <- rbind(previous_data, latest_data) |>
  dplyr::arrange(desc(date))


# COMMAND ----------

# DBTITLE 1,Quick data integrity checks

test_that("New data has more rows than previous data", {
  expect_true(nrow(updated_data) >= nrow(previous_data))
  expect_false(nrow(updated_data) == nrow(previous_data))
})

test_that("New data has no duplicate rows", {
  expect_true(nrow(updated_data) == nrow(dplyr::distinct(updated_data)))
})

test_that("Latest date is as expected", {
  expect_equal(updated_data$date[1], Sys.Date() - 3)
})

test_that("Data has no missing values", {
  expect_false(any(is.na(updated_data)))

  # 2023-06-22 is the first date we collected data for
  expect_equal(
    updated_data$date,
    seq(as.Date("2023-06-22"), Sys.Date() - 3, by = "day")
  )
})

# COMMAND ----------

# DBTITLE 1,Write to table

# Create a Spark DataFrame from ga4_data
ga4_spark_df <- createDataFrame(updated_data)

# Save the DataFrame as a table in the metastore
saveAsTable(updated_data, table_name, mode = "overwrite")
