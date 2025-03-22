# Databricks notebook source
# DBTITLE 1,Load dependencies
source("utils.R")

packages <- c("sparklyr", "DBI", "dplyr", "testthat", "arrow", "stringr")

install_if_needed(packages)
lapply(packages, library, character.only = TRUE)

ga4_event_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_ga4_events"
ua_event_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_ua_events"
scrape_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_pub_scrape"
write_table_name <- "catalog_40_copper_statistics_services.analytics_app.ees_session_starts"

sc <- spark_connect(method = "databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The session_start event was new for GA4. 
# MAGIC
# MAGIC **eventName**
# MAGIC
# MAGIC - session_start (logs the pagePath where sessions began)
# MAGIC
# MAGIC **eventLabel** in not set so not needed
# MAGIC
# MAGIC **eventCategory** in not set so not needed
# MAGIC
# MAGIC NOTE:
# MAGIC We don't have an equivalent event for universal analytics. 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Join the tables together and filter to just accordion relevant events
session_starts <- sparklyr::sdf_sql(
  sc, paste("
      SELECT 
      date, 
      pagePath,
      eventName,
      sum(eventCount) as eventCount
      FROM ", ga4_event_table_name, "
    WHERE eventName = 'session_start'
    GROUP BY date, pagePath, eventName
    ORDER BY date DESC
  ")
) %>% collect()


# COMMAND ----------

# DBTITLE 0,Join together and check table integrity
test_that("No duplicate rows", {
  expect_true(nrow(session_starts) == nrow(dplyr::distinct(session_starts)))
})

test_that("Data has no missing values", {
  expect_false(any(is.na(session_starts)))
})

dates <- create_dates(max(session_starts$date))

test_that("There are no missing dates since we started", {
  expect_equal(
    setdiff(session_starts$date, seq(as.Date(dates$all_time_date), max(dates$latest_date), by = "day")) |>
      length(),
    0
  )
})


# COMMAND ----------

# DBTITLE 1,Adding a page_type column to help distinguish between different types of search
session_starts <- session_starts %>%
  mutate(page_type = case_when(
    str_detect(pagePath, "/find-statistics/") ~ "Release page",
    str_detect(pagePath, "/find-statistics") ~ "Find stats navigation",
    str_detect(pagePath, "/data-catalogue/data-set") ~ "Data catalogue dataset",
    str_detect(pagePath, "/data-catalogue") ~ "Data catalogue navigation",
    str_detect(pagePath, "/data-tables/permalink") ~ "Permalink",
    str_detect(pagePath, "/data-tables/") ~ "Table tool",
    str_detect(pagePath, "/methodology/") ~ "Methodology page",
    str_detect(pagePath, "/methodology") ~ "Methodology navigation",
    str_detect(pagePath, "/subscriptions/") ~ "Subscriptions",
    str_detect(pagePath, "/glossary") ~ "Glossary",
    str_detect(pagePath, "/cookies") ~ "Cookies",
    str_detect(pagePath, "/") ~ "Homepage",
    TRUE ~ 'NA'
  ))

# COMMAND ----------

# DBTITLE 1,Tests
test_that("There are no events without a page type classification", {
    expect_true(nrow(session_starts %>% filter(page_type =='NA')) == 0)
    })

# COMMAND ----------

# DBTITLE 1,Bringing in scraped publications list
scraped_publications <- sparklyr::sdf_sql(sc, paste("SELECT * FROM", scrape_table_name)) |> collect()

slugs <- unique(scraped_publications$slug)

# COMMAND ----------

# DBTITLE 1,Joining publication info
# Joining publication info onto the publication specific events
session_starts <- session_starts |>
  mutate(slug = str_remove(pagePath, "^/(methodology|find-statistics|data-catalogue|data-tables|subscriptions)/")) |>
  mutate(slug = str_remove(slug, "-methodology")) |>
  mutate(slug = str_remove(slug, "/.*")) |>
  mutate(slug = str_trim(slug, side = "both")) |>
  mutate(slug = str_remove(slug, "\\.$")) |>
  mutate(slug = str_to_lower(slug)) |>
  left_join(scraped_publications, by = c("slug" = "slug")) |>
  rename("publication" = title)

dates <- create_dates(max(session_starts$date))

test_that("There are no missing dates since we started", {
  expect_equal(
    setdiff(session_starts$date, seq(as.Date(dates$all_time_date), max(dates$latest_date), by = "day")) |>
      length(),
    0
  )
})

# COMMAND ----------

# selecting just the columns we're interested in storing
session_starts <- session_starts %>%
select(date, pagePath, page_type, publication, eventCount)

# COMMAND ----------

# DBTITLE 1,Write out app data
updated_spark_df <- copy_to(sc, session_starts, overwrite = TRUE)

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
  expect_equal(nrow(temp_table_data), nrow(session_starts))
})

# Replace the old table with the new one
dbExecute(sc, paste0("DROP TABLE IF EXISTS ", write_table_name))
dbExecute(sc, paste0("ALTER TABLE ", write_table_name, "_temp RENAME TO ", write_table_name))

print_changes_summary(temp_table_data, previous_data)

# COMMAND ----------

# MAGIC %md
# MAGIC We're left with the following table 
# MAGIC
# MAGIC - **date**: The date the session started 
# MAGIC - **pagePath**: The pagePath where the session started
# MAGIC - **page_type**: Type of service page (Release page, Data catalogue, glossary etc)
# MAGIC - **publication**: The publication title (relevant for pages that have an associated publication only)
# MAGIC - **eventCount**: The number of session starts for that page on given day
# MAGIC
