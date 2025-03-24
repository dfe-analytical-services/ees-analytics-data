# Databricks notebook source
# DBTITLE 1,Load dependencies
source("utils.R")

packages <- c("sparklyr", "DBI", "dplyr", "testthat", "arrow", "stringr")

install_if_needed(packages)
lapply(packages, library, character.only = TRUE)

ga4_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_ga4_source_medium"
ua_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_ua_source_medium"
scrape_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_pub_scrape"
write_table_name <- "catalog_40_copper_statistics_services.analytics_app.ees_source_medium"

sc <- spark_connect(method = "databricks")

# COMMAND ----------

# DBTITLE 1,Read in and check table integrity
# Have to do the group by to avoid duplicates because of the change over day. It's not ideal for avgtimeonpage or bouncerate but given the imact is just on that day I'm not going to worry too much about it for now. 

full_data <- sparklyr::sdf_sql(sc, paste("
  SELECT 
   date, pagePath, source, medium, SUM(pageviews) as pageviews, SUM(sessions) as sessions, AVG(avgTimeOnPage) as avgTimeOnPage, AVG(bounceRate) as bounceRate
  FROM (
    SELECT 
    date, pagePath, source, medium, pageviews, sessions, avgTimeOnPage, bounceRate FROM", ua_table_name, "
    UNION ALL
    SELECT 
    date, pagePath, sessionSource as source, sessionMedium as medium, pageviews, sessions, avgTimeOnPage, bounceRate FROM", ga4_table_name, "
  ) AS combined_data
  GROUP BY date, pagePath, source, medium
  ORDER BY date DESC
")) %>% collect()

test_that("No duplicate rows", {
  expect_true(nrow(full_data) == nrow(dplyr::distinct(full_data)))
})

test_that("Data has no missing values", {
  expect_false(any(is.na(full_data)))
})

dates <- create_dates(max(full_data$date))

test_that("There are no missing dates since we started", {
  expect_equal(
    setdiff(full_data$date, seq(as.Date(dates$all_time_date), max(dates$latest_date), by = "day")) |>
      length(),
    0
  )
})

# COMMAND ----------

# DBTITLE 1,Filter table down to only publication and release pages
scraped_publications <- sparklyr::sdf_sql(sc, paste("SELECT * FROM", scrape_table_name)) |> collect()

slugs <- unique(scraped_publications$slug)

# COMMAND ----------

# DBTITLE 1,Create a slug column and join on publication titles
joined_data <- full_data |>
  mutate(slug = str_remove(pagePath, "^/(methodology|find-statistics|data-tables|data-catalogue)/")) |>
  mutate(slug = str_remove(slug, "-methodology")) |>
  mutate(slug = str_remove(slug, "/.*")) |>
  mutate(slug = str_trim(slug, side = "both")) |>
  mutate(slug = str_to_lower(slug)) |>
  left_join(scraped_publications, by = c("slug" = "slug")) |>
  rename("publication" = title) |>
  mutate(publication = str_to_title(publication))

dates <- create_dates(max(full_data$date))

test_that("There are no missing dates since we started", {
  expect_equal(
    setdiff(full_data$date, seq(as.Date(dates$all_time_date), max(dates$latest_date), by = "day")) |>
      length(),
    0
  )
})

# COMMAND ----------

joined_data <- joined_data %>% select(date, pagePath, publication, source, medium, pageviews, sessions, avgTimeOnPage, bounceRate)

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

# COMMAND ----------

# MAGIC %md
# MAGIC NOTE: 
# MAGIC
# MAGIC We could clean some dodgy pagePagepaths out of this but have left for now whilst it includes servive as a whole and specific publications. 
# MAGIC Remember if aggregating up from this level avgtimeonpage and bouncerate will no longer be accurate. To aggregate and have an accurate time on page we'd need to shorten time series to just GA4 data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC BIG NOTE - need to actually update this one as I forgot but the documentation is here when it's ready :) 
# MAGIC
# MAGIC **Note:**
# MAGIC
# MAGIC We are left with two tables:
# MAGIC
# MAGIC 1. **Publication Table**
# MAGIC    For release pages only.
# MAGIC    - date
# MAGIC    - publication
# MAGIC    - source
# MAGIC    - medium
# MAGIC    - pageviews
# MAGIC    - sessions
# MAGIC
# MAGIC 2. **Service Table**
# MAGIC    For all service pages.
# MAGIC    - date
# MAGIC    - page_type
# MAGIC    - source
# MAGIC    - medium
# MAGIC    - pageviews
# MAGIC    - sessions
