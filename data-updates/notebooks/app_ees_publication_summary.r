# Databricks notebook source
# DBTITLE 1,Load dependencies
source("utils.R")

packages <- c("sparklyr", "DBI", "dplyr", "testthat", "arrow", "stringr")

install_if_needed(packages)
lapply(packages, library, character.only = TRUE)

ga4_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_ga4_page"
ua_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_ua_page"
scrape_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_pub_scrape"
write_table_name <- "catalog_40_copper_statistics_services.analytics_app.ees_publication_summary"

sc <- spark_connect(method = "databricks")

# COMMAND ----------

# DBTITLE 1,Read in and check table integrity
sql_data <- sparklyr::sdf_sql(sc, paste("
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
  expect_true(nrow(sql_data) == nrow(dplyr::distinct(sql_data)))
})

test_that("Data has no missing values", {
  expect_false(any(is.na(sql_data)))
})

dates <- create_dates(max(sql_data$date))

test_that("There are no missing dates since we started", {
  expect_equal(
    setdiff(sql_data$date, seq(as.Date(dates$all_time_date), max(dates$latest_date), by = "day")) |>
      length(),
    0
  )
})

# COMMAND ----------

scraped_publications <- sparklyr::sdf_sql(sc, paste("SELECT * FROM", scrape_table_name)) |> collect()

expected_dates <- seq(as.Date(dates$all_time_date), max(dates$latest_date), by = "day")
expected_publications <- data.frame(publication = scraped_publications$title)

expected_df <- expand.grid(
  date = expected_dates,
  publication = unique(str_to_title(expected_publications$publication))
)

# COMMAND ----------

# DBTITLE 1,Filter table down to only publication and release pages
slugs <- unique(scraped_publications$slug)
possible_suffixes <- c("/methodology", "/data-guidance", "/prerelease-access-list")

filtered_data <- sql_data |>
  filter(str_detect(pagePath, "/find-statistics/")) |>
  filter(str_detect(pagePath, paste(slugs, collapse = "|"))) |>
  filter(!str_detect(pagePath, paste(possible_suffixes, collapse = "|")))

# COMMAND ----------

# MAGIC %md
# MAGIC Decided to aggregate up to publication level here to reduce the rows.
# MAGIC
# MAGIC Currently for the app, we can leave it up to the publication teams to look at dates and think about separate releases by date.

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

pub_agg_data <- joined_data |>
  group_by(date, publication) |>
  summarise(
    pageviews = sum(pageviews),
    sessions = sum(sessions),
    .groups = "keep"
  ) |>
  mutate(publication = str_to_title(publication))

dates <- create_dates(max(sql_data$date))

test_that("There are no missing dates since we started", {
  expect_equal(
    setdiff(pub_agg_data$date, seq(as.Date(dates$all_time_date), max(dates$latest_date), by = "day")) |>
      length(),
    0
  )
})

# COMMAND ----------

session_start_events <- sparklyr::sdf_sql(sc, "SELECT date, publication, SUM(eventCount) AS total_session_starts FROM catalog_40_copper_statistics_services.analytics_app.ees_session_starts WHERE page_type = 'Release page' and publication is not null GROUP BY publication, date") %>%
  collect() ## total should be 3048542

accordion_events <- sparklyr::sdf_sql(sc, "SELECT date, publication, SUM(eventCount) AS total_accordion_events FROM catalog_40_copper_statistics_services.analytics_app.ees_publication_accordions WHERE page_type = 'Release page' and publication is not null GROUP BY publication, date") %>%
  collect() ## total should be 3318718

download_events <- sparklyr::sdf_sql(sc, "SELECT date, publication, SUM(eventCount) AS total_download_events FROM catalog_40_copper_statistics_services.analytics_app.ees_publication_downloads WHERE publication is not null GROUP BY publication, date") %>%
  collect() ## total should be 779468

featured_table_events <- sparklyr::sdf_sql(sc, "SELECT date, publication, SUM(eventCount) AS total_featured_tables FROM catalog_40_copper_statistics_services.analytics_app.ees_publication_featured_tables WHERE publication is not null GROUP BY publication, date") %>%
  collect() ## total should be 184397

search_events <- sparklyr::sdf_sql(sc, "SELECT date, publication, SUM(eventCount) AS total_search_events FROM catalog_40_copper_statistics_services.analytics_app.ees_publication_search_events WHERE page_type = 'Release page' and publication is not null GROUP BY publication, date") %>%
  collect() ## total should be 195487

tables_created <- sparklyr::sdf_sql(sc, "SELECT date, publication, SUM(eventCount) AS total_tables_created FROM catalog_40_copper_statistics_services.analytics_app.ees_publication_tables_created WHERE publication is not null GROUP BY publication, date") %>%
  collect() ## total should be 712225

# COMMAND ----------

with_event_totals <- expected_df |>
  left_join(pub_agg_data, by = c("publication" = "publication", "date" = "date")) |>
  left_join(session_start_events, by = c("publication" = "publication", "date" = "date")) |>
  left_join(accordion_events, by = c("publication" = "publication", "date" = "date")) |>
  left_join(download_events, by = c("publication" = "publication", "date" = "date")) |>
  left_join(featured_table_events, by = c("publication" = "publication", "date" = "date")) |>
  left_join(search_events, by = c("publication" = "publication", "date" = "date")) |>
  left_join(tables_created, by = c("publication" = "publication", "date" = "date"))

# COMMAND ----------

# DBTITLE 1,Write out app data
updated_spark_df <- copy_to(sc, with_event_totals, overwrite = TRUE)

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
  expect_equal(nrow(temp_table_data), nrow(with_event_totals))
})

# Replace the old table with the new one
dbExecute(sc, paste0("DROP TABLE IF EXISTS ", write_table_name))
dbExecute(sc, paste0("ALTER TABLE ", write_table_name, "_temp RENAME TO ", write_table_name))

print_changes_summary(temp_table_data, previous_data)
