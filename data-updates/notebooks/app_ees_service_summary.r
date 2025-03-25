# Databricks notebook source
# DBTITLE 1,Load dependencies
source("utils.R")

packages <- c("sparklyr", "DBI", "dplyr", "testthat", "arrow", "TTR")

install_if_needed(packages)
lapply(packages, library, character.only = TRUE)

ga4_service_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_ga4_service_summary"
ua_service_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_ua_service_summary"

write_table_name <- "catalog_40_copper_statistics_services.analytics_app.ees_service_summary"

sc <- spark_connect(method = "databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC While there is a column for sessions in the pages tables, it's important to understand that that column refers to the number of sessions that touched that page. As there can be multiple pages in a session, aggregating that column overcounts the number of sessions, so we need to pull in sessions data from a dedicated sessions table and join that on.

# COMMAND ----------

# DBTITLE 1,Join together and check table integrity
aggregated_data <- sparklyr::sdf_sql(
  sc, paste("
    SELECT date, pageviews, sessions, avg_session_duration as averageSessionDuration FROM", ua_service_table_name, "
      UNION ALL
    SELECT date, screenPageViews as pageviews, sessions, averageSessionDuration FROM", ga4_service_table_name, "
    ORDER BY date DESC;
  ")
) %>% collect()

# Create rolling averages (simple moving average)
aggregated_data <- aggregated_data %>%
  arrange(date) %>%
  mutate(
    pageviews_avg7 = TTR::SMA(pageviews, n = 7),
    sessions_avg7 = TTR::SMA(sessions, n = 7),
    pagesPerSession = pageviews / sessions
  )

test_that("No duplicate rows", {
  expect_true(nrow(aggregated_data) == nrow(dplyr::distinct(aggregated_data)))
})

test_that("Data has no missing values", {
  # We expect the rolling average cols to have some missing values so ignoring for this
  expect_false(any(is.na(aggregated_data |> select(-c(pageviews_avg7, sessions_avg7)))))
})

dates <- create_dates(max(aggregated_data$date))

test_that("There are no missing dates since we started", {
  expect_equal(
    setdiff(aggregated_data$date, seq(as.Date(dates$all_time_date), max(dates$latest_date), by = "day")) |>
      length(),
    0
  )
})

# COMMAND ----------

# DBTITLE 1,Write out app data
updated_spark_df <- copy_to(sc, aggregated_data, overwrite = TRUE)

# Write to temp table while we confirm we're good to overwrite data
spark_write_table(updated_spark_df, paste0(write_table_name, "_temp"), mode = "overwrite")

temp_table_data <- sparklyr::sdf_sql(sc, paste0("SELECT * FROM ", write_table_name, "_temp")) %>% collect()
previous_data <- tryCatch(
  {
    sparklyr::sdf_sql(sc, paste0("SELECT * FROM ", write_table_name)) %>% collect()
  },
  error = function(e) {
    NULL
  }
)

test_that("Temp table data matches updated data", {
  expect_equal(temp_table_data, aggregated_data)
})

# Replace the old table with the new one
dbExecute(sc, paste0("DROP TABLE IF EXISTS ", write_table_name))
dbExecute(sc, paste0("ALTER TABLE ", write_table_name, "_temp RENAME TO ", write_table_name))

print_changes_summary(temp_table_data, previous_data)
