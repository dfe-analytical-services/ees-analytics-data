# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # BE AWARE THIS NOTEBOOK MAKES MORE THAN ONE TABLE!
# MAGIC
# MAGIC This notebook currently grabs the top 10 searches in various ways, though a future improvement is to do some analysis of the most common words, as this doesn't yet account for all of the similar searches. Worth having a look at the text mining packages for this like [tm](https://tm.r-forge.r-project.org/).

# COMMAND ----------

source("utils.R")

packages <- c("sparklyr", "DBI", "dplyr", "testthat", "arrow", "stringr")

install_if_needed(packages)
lapply(packages, library, character.only = TRUE)

search_console_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_search_console"
scrape_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_pub_scrape"
queries_table_name <- "catalog_40_copper_statistics_services.analytics_app.ees_search_console_queries"

sc <- spark_connect(method = "databricks")

# COMMAND ----------

# DBTITLE 1,Pull in raw data
full_data <- sparklyr::sdf_sql(
  sc, paste("
    SELECT * FROM", search_console_table_name,
    "WHERE date >= DATE_SUB(CURRENT_DATE(), 365);
  ")
) %>% collect()

# COMMAND ----------

scraped_publications <- sparklyr::sdf_sql(sc, paste("SELECT * FROM", scrape_table_name)) |> 
  collect()

slugs <- unique(scraped_publications$slug)
possible_suffixes <- c("/methodology", "/data-guidance", "/prerelease-access-list")

# COMMAND ----------

filtered_data <- full_data |>
  mutate(pagePath = str_remove(pagePath, "^https://explore-education-statistics.service.gov.uk")) |>
  filter(str_detect(pagePath, "/find-statistics/")) |>
  filter(str_detect(pagePath, paste(slugs, collapse = "|"))) |>
  filter(!str_detect(pagePath, paste(possible_suffixes, collapse = "|")))

# COMMAND ----------

joined_data <- filtered_data |>
  mutate(slug = str_remove(pagePath, "^/find-statistics/")) |>
  mutate(slug = str_remove(slug, "/.*")) |>
  mutate(slug = str_remove(slug, "\\.$")) |>
  left_join(scraped_publications, by = c("slug" = "slug")) |>
  rename("publication" = title) |>
  # this drops a raft of dodgy URLs like '/find-statistics/school-workforce-in-england)'
  filter(!is.na(publication)) |>
  select(date, query, publication, clicks, impressions)

# COMMAND ----------

pub_queries_clicks <- data.frame()

for(pub in unique(scraped_publications$title)){
  message("Finding top 10 clicks for ", pub)

  pub_queries_clicks <- rbind(
    pub_queries_clicks,
    joined_data |>
      filter(publication == pub) |>
      group_by(publication, query) |>
      summarise(count = sum(clicks), .groups = "keep") |>
      arrange(desc(count)) |>
      head(10) |>
      mutate(metric = "clicks")
  )
}

pub_queries_impressions <- data.frame()

for(pub in unique(scraped_publications$title)){
  message("Finding top 10 impressions for ", pub)

  pub_queries_impressions <- rbind(
    pub_queries_impressions,
    joined_data |>
      filter(publication == pub) |>
      group_by(publication, query) |>
      summarise(count = sum(impressions), .groups = "keep") |>
      arrange(desc(count)) |>
      head(10) |>
      mutate(metric = "impressions")
  )
}


# COMMAND ----------

top_clicks <- full_data |>
  group_by(query) |>
  summarise(count = sum(clicks)) |>
  arrange(desc(count)) |>
  head(10) |>
  mutate(publication = "Service", metric = "clicks")

top_impressions <- full_data |>
  group_by(query) |>
  summarise(count = sum(impressions)) |>
  arrange(desc(count)) |>
  head(10) |>
  mutate(publication = "Service", metric = "impressions")

combined_data <- bind_rows(top_clicks, top_impressions, pub_queries_clicks, pub_queries_impressions)

# COMMAND ----------

updated_spark_df <- copy_to(sc, combined_data, overwrite = TRUE)

# Write to temp table while we confirm we're good to overwrite data
spark_write_table(updated_spark_df, paste0(queries_table_name, "_temp"), mode = "overwrite")

temp_table_data <- sparklyr::sdf_sql(sc, paste0("SELECT * FROM ", queries_table_name, "_temp")) %>% collect()
previous_data <- tryCatch(
  {
    sparklyr::sdf_sql(sc, paste0("SELECT * FROM ", queries_table_name)) %>% collect()
  },
  error = function(e) {
    NULL
  }
)

test_that("Temp table data matches updated data", {
  expect_equal(nrow(temp_table_data), nrow(combined_data))
})

# Replace the old table with the new one
dbExecute(sc, paste0("DROP TABLE IF EXISTS ", queries_table_name))
dbExecute(sc, paste0("ALTER TABLE ", queries_table_name, "_temp RENAME TO ", queries_table_name))

print_changes_summary(temp_table_data, previous_data)

# COMMAND ----------

# MAGIC %md
# MAGIC # Additional table!
# MAGIC
# MAGIC Normally I'd do a separate notebook per table, though given we have the data in memory here it felt more efficient to make the additional table here instead.

# COMMAND ----------

time_series_table_name <- "catalog_40_copper_statistics_services.analytics_app.ees_search_console_timeseries"
search_console_old_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_search_console_old"

# COMMAND ----------

latest_data <- sparklyr::sdf_sql(
  sc, paste("
    SELECT date, SUM(clicks) as clicks, SUM(impressions) as impressions FROM", search_console_table_name,
    "GROUP BY date;
  ")
)

old_data <- sparklyr::sdf_sql(
  sc, paste("
    SELECT date, SUM(clicks) as clicks, SUM(impressions) as impressions FROM", search_console_old_table_name,
    "GROUP BY date;
  ")
)

combined_data <- union_all(latest_data, old_data) |>
  arrange(desc(date)) |>
  collect()

# COMMAND ----------

updated_spark_df <- copy_to(sc, combined_data, overwrite = TRUE)

# Write to temp table while we confirm we're good to overwrite data
spark_write_table(updated_spark_df, paste0(time_series_table_name, "_temp"), mode = "overwrite")

temp_table_data <- sparklyr::sdf_sql(sc, paste0("SELECT * FROM ", time_series_table_name, "_temp")) %>% collect()
previous_data <- tryCatch(
  {
    sparklyr::sdf_sql(sc, paste0("SELECT * FROM ", time_series_table_name)) %>% collect()
  },
  error = function(e) {
    NULL
  }
)

test_that("Temp table data matches updated data", {
  expect_equal(nrow(temp_table_data), nrow(combined_data))
})

# Replace the old table with the new one
dbExecute(sc, paste0("DROP TABLE IF EXISTS ", time_series_table_name))
dbExecute(sc, paste0("ALTER TABLE ", time_series_table_name, "_temp RENAME TO ", time_series_table_name))

print_changes_summary(temp_table_data, previous_data)
