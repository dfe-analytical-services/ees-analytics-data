# Databricks notebook source
# DBTITLE 1,Load dependencies
source("utils.R")

packages <- c("sparklyr", "DBI", "dplyr", "testthat", "arrow", "stringr")

install_if_needed(packages)
lapply(packages, library, character.only = TRUE)

ga4_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_ga4_page"
ua_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_ua_page"
scrape_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_pub_scrape"
write_table_name <- "catalog_40_copper_statistics_services.analytics_app.ees_pub_page"

sc <- spark_connect(method = "databricks")

# COMMAND ----------

# DBTITLE 1,Read in and check table integrity
aggregated_data <- sparklyr::sdf_sql(sc, paste("
  SELECT date, pagePath, SUM(pageviews) AS pageviews, SUM(sessions) AS sessions
  FROM (
    SELECT date, pagePath, pageviews, sessions FROM", ua_table_name, "
    UNION ALL
    SELECT date, pagePath, pageviews, sessions FROM", ga4_table_name, "
  ) AS combined_data
  GROUP BY date, pagePath
  ORDER BY date DESC
")) %>% collect()

test_that("No duplicate rows", {
  expect_true(nrow(aggregated_data) == nrow(dplyr::distinct(aggregated_data)))
})

test_that("Data has no missing values", {
  expect_false(any(is.na(aggregated_data)))
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

# DBTITLE 1,Filter table down to only publication and release pages
scraped_publications <- sparklyr::sdf_sql(sc, paste("SELECT * FROM", scrape_table_name)) |> collect()

slugs <- unique(scraped_publications$slug)
possible_suffixes <- c("/methodology", "/data-guidance", "/prerelease-access-list")

filtered_data <- aggregated_data |>
  filter(str_detect(pagePath, "/find-statistics/")) |>
  filter(str_detect(pagePath, paste(slugs, collapse = "|"))) |>
  filter(!str_detect(pagePath, paste(possible_suffixes, collapse = "|")))

# COMMAND ----------

# DBTITLE 1,Create a slug column and join on publication titles
joined_data <- filtered_data |>
  mutate(slug = str_remove(pagePath, "^/find-statistics/")) |>
  mutate(slug = str_remove(slug, "/.*")) |>
  mutate(slug = str_remove(slug, "\\.$")) |>
  left_join(scraped_publications, by = c("slug" = "slug")) |>
  rename("publication" = title) |>
  # this drops a raft of dodgy URLs like '/find-statistics/school-workforce-in-england)'
  filter(!is.na(publication)) |> 
  select(date, pagePath, publication, pageviews, sessions)

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
updated_spark_df <- copy_to(sc, joined_data, overwrite = TRUE)

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
  expect_equal(temp_table_data, joined_data)
})

# Replace the old table with the new one
dbExecute(sc, paste0("DROP TABLE IF EXISTS ", write_table_name))
dbExecute(sc, paste0("ALTER TABLE ", write_table_name, "_temp RENAME TO ", write_table_name))

print_changes_summary(temp_table_data, previous_data)
