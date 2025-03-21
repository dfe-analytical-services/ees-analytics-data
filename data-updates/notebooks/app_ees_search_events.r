# Databricks notebook source
# DBTITLE 1,Load dependencies
source("utils.R")

packages <- c("sparklyr", "DBI", "dplyr", "testthat", "arrow", "stringr")

install_if_needed(packages)
lapply(packages, library, character.only = TRUE)

ga4_event_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_ga4_events"
ua_event_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_ua_events"
scrape_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_pub_scrape"
write_table_name <- "catalog_40_copper_statistics_services.analytics_app.ees_search_events"

sc <- spark_connect(method = "databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC For search events in GA4 we need:
# MAGIC
# MAGIC **eventName**
# MAGIC
# MAGIC - PageSearchForm (used for release and methoodlogy pages)
# MAGIC - Publications Filtered by Search (used for find stats page)
# MAGIC - Data Sets Filtered by searchTerm (used for data catalogue)
# MAGIC
# MAGIC **eventLabel** gives the search term used
# MAGIC
# MAGIC **eventCategory** gives either the page title or slug of the page the search event occured on
# MAGIC
# MAGIC NOTE:
# MAGIC We also have tracking for the following eventName's but are conciously not including them here
# MAGIC - Reset Search Filter
# MAGIC - Reset searchTerm Filter
# MAGIC - Clear Search Filter
# MAGIC - Clear searchTerm Filter
# MAGIC
# MAGIC NOTE: 
# MAGIC We don't seem to be tracking any searches on the table tool
# MAGIC
# MAGIC For search events in UA we need: 
# MAGIC
# MAGIC **eventAction**
# MAGIC - PageSearchForm (used for release and methodology pages -- and /find-stats/ by the looks of things which we might need to check alongside 'Publications Filtered By Search')
# MAGIC - Publications Filtered by Search (used for find stats page)
# MAGIC
# MAGIC **eventLabel** gives the search term used
# MAGIC
# MAGIC **eventCategory** gives either the page title or slug of the page the search event occured on
# MAGIC
# MAGIC NOTE:
# MAGIC We also have tracking for the following eventName's but are conciously not including them here
# MAGIC - Clear Search Filter

# COMMAND ----------

# DBTITLE 1,Join the tables together and filter to just accordion relevant events

## I've done this with a group by because otherwise the tests would fail because of duplicates. This could mean I'm just double counting on the day overlap between the two tables - but would need to investigate more to find out.

full_data <- sparklyr::sdf_sql(
  sc, paste("
    SELECT 
      date, 
      pagePath,
      eventName,
      eventLabel,
      eventCategory,
      SUM(eventCount) as eventCount
    FROM (
      SELECT 
      date, 
      pagePath,
      eventAction as eventName,
      eventLabel,
      eventCategory, 
      totalEvents as eventCount     
      FROM ", ua_event_table_name, "
      UNION ALL
      SELECT 
      date, 
      pagePath,
      eventName,
      eventLabel,
      eventCategory,
      eventCount 
      FROM ", ga4_event_table_name, "
    ) AS p
    GROUP BY date, pagePath, eventName, eventLabel, eventCategory
    ORDER BY date DESC
  ")
) %>% collect()

search_events <- full_data %>% filter(eventName %in% c('PageSearchForm', 'Publications Filtered by Search', 'Data Sets Filtered by searchTerm'))


# COMMAND ----------

# DBTITLE 0,Join together and check table integrity
test_that("No duplicate rows", {
  expect_true(nrow(search_events) == nrow(dplyr::distinct(search_events)))
})

test_that("Data has no missing values", {
  expect_false(any(is.na(search_events)))
})

dates <- create_dates(max(search_events$date))

test_that("There are no missing dates since we started", {
  expect_equal(
    setdiff(search_events$date, seq(as.Date(dates$all_time_date), max(dates$latest_date), by = "day")) |>
      length(),
    0
  )
})


# COMMAND ----------

# DBTITLE 1,Adding a page_type column to help distinguish between different types of search
search_events <- search_events %>%
  mutate(page_type = case_when(
    str_detect(eventCategory, "/Find-Statistics/") ~ "Release page",
    
    str_detect(eventCategory, "Find Statistics and Data") ~ "Find stats",
    str_detect(eventCategory, "/Find-Statistics") ~ "Find stats",
    
    str_detect(eventCategory, "Glossary") ~ "Glossary",

    str_detect(eventCategory, "Data Catalogue") ~ "Data catalogue",
    str_detect(eventCategory, "/Data-Catalogue/") ~ "Data catalogue",
    str_detect(eventCategory, "/Download-Latest-Data") ~ "Data catalogue",

    str_detect(eventCategory, "/Methodology/") ~ "Methodology pages",
    str_detect(eventCategory, "/Methodology") ~ "Methodology nav",

    str_detect(eventCategory, "/Data-Tables/") ~ "Table tool",
    TRUE ~ 'NA'
  ))


# COMMAND ----------

# DBTITLE 1,Tests
test_that("There are no events without a page type classification", {
    expect_true(nrow(search_events %>% filter(page_type =='NA')) == 0)
    })

# COMMAND ----------

# DBTITLE 1,Bringing in scraped publications list
scraped_publications <- sparklyr::sdf_sql(sc, paste("SELECT * FROM", scrape_table_name)) |> collect()

slugs <- unique(scraped_publications$slug)

# COMMAND ----------

# DBTITLE 1,Joining publication info
# Joining publication info onto the publication specific events
search_events <- search_events |>
  mutate(slug = str_remove(pagePath, "^/(methodology|find-statistics|data-catalogue|data-tables)/")) |>
  mutate(slug = str_remove(slug, "-methodology")) |>
  mutate(slug = str_remove(slug, "/.*")) |>
  mutate(slug = str_trim(slug, side = "both")) |>
  mutate(slug = str_remove(slug, "\\.$")) |>
  mutate(slug = str_to_lower(slug)) |>
  left_join(scraped_publications, by = c("slug" = "slug")) |>
  rename("publication" = title)

dates <- create_dates(max(search_events$date))

test_that("There are no missing dates since we started", {
  expect_equal(
    setdiff(search_events$date, seq(as.Date(dates$all_time_date), max(dates$latest_date), by = "day")) |>
      length(),
    0
  )
})

# COMMAND ----------

# selecting just the columns we're interested in storing
# TO DO: decide if we only want subsets of page_types in here (e.g make it just about publications or remove defunct pages like data catalogue)

search_events <- search_events %>%
select(date, pagePath, page_type, publication, eventLabel, eventCount)

# COMMAND ----------

# DBTITLE 1,Write out app data
updated_spark_df <- copy_to(sc, search_events, overwrite = TRUE)

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
  expect_equal(temp_table_data, search_events)
})

# Replace the old table with the new one
dbExecute(sc, paste0("DROP TABLE IF EXISTS ", write_table_name))
dbExecute(sc, paste0("ALTER TABLE ", write_table_name, "_temp RENAME TO ", write_table_name))

print_changes_summary(temp_table_data, previous_data)

# COMMAND ----------

# MAGIC %md
# MAGIC We're left with the following table 
# MAGIC
# MAGIC - **date**: The date the event occured on (earliest date = 21/04/2021)
# MAGIC - **pagePath**: The pagePath the event occured on
# MAGIC - **page_type**: Type of service page (Release page, Data catalogue, glossary etc)
# MAGIC - **publication**: The publication title (relevant for pages that have an associated publication only)
# MAGIC - **eventLabel**: The search term used
# MAGIC - **eventCount**: The number of searches for that term on given day
# MAGIC
