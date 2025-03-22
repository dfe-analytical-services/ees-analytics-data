# Databricks notebook source
# DBTITLE 1,Load dependencies
source("utils.R")

packages <- c("sparklyr", "DBI", "dplyr", "testthat", "arrow", "stringr")

install_if_needed(packages)
lapply(packages, library, character.only = TRUE)

ga4_event_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_ga4_events"
ua_event_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_ua_events"
scrape_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_pub_scrape"
write_table_name <- "catalog_40_copper_statistics_services.analytics_app.ees_tables_created"

sc <- spark_connect(method = "databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC NOTE:
# MAGIC We originally thought the 'Publication and Subject chosen event tracked the firs ttwo steps of the table tool only and 'Table Created' was tracking the final create table button (so successful journies through table tool). This doesn't seem to be the case as the numbers are suspiciously similar across the two categories (exactly the same in most cases). 
# MAGIC Current working assumption is they both trigger when a table is displayed, but the 'Publication and Sugbject' chosen event logs more information. 
# MAGIC I'll use the Publication and Subject chosen event here.
# MAGIC
# MAGIC For table creation via table tool events in GA4 we need:
# MAGIC
# MAGIC **eventName**
# MAGIC
# MAGIC - Publication and Subject Chosen
# MAGIC
# MAGIC **eventLabel** gives publication and table name
# MAGIC
# MAGIC **eventCategory** gives ...
# MAGIC
# MAGIC For table creation via table tool events in UA we need:
# MAGIC
# MAGIC **eventAction**
# MAGIC - Publication and Subject Chosen
# MAGIC
# MAGIC **eventLabel** gives publication and table name
# MAGIC
# MAGIC **eventCategory** gives ...
# MAGIC

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

tables_created <- full_data %>%
  filter(
    eventName %in% c("Publication and Subject Chosen")
  )


# COMMAND ----------

# DBTITLE 0,Join together and check table integrity
test_that("No duplicate rows", {
  expect_true(nrow(tables_created) == nrow(dplyr::distinct(tables_created)))
})

test_that("Data has no missing values", {
  expect_false(any(is.na(tables_created)))
})

dates <- create_dates(max(tables_created$date))

test_that("There are no missing dates since we started", {
  expect_equal(
    setdiff(tables_created$date, seq(as.Date(dates$all_time_date), max(dates$latest_date), by = "day")) |>
      length(),
    0
  )
})


# COMMAND ----------

# DBTITLE 1,Adding a page_type column to help distinguish between different types of download
tables_created <- tables_created %>%
  mutate(page_type = case_when(
    str_detect(pagePath, "find-statistics") ~ "Release page",
    str_detect(eventCategory , "Table Tool") ~ "Table tool",
    TRUE ~ "NA"
  ))


# COMMAND ----------

test_that("There are no events without a page type classification", {
  expect_true(nrow(tables_created %>% filter(page_type == "NA")) == 0)
})

# COMMAND ----------

# DBTITLE 1,Bringing in scraped publications list
scraped_publications <- sparklyr::sdf_sql(sc, paste("SELECT * FROM", scrape_table_name)) |> collect()

slugs <- unique(scraped_publications$slug)

# COMMAND ----------

# DBTITLE 1,Joining publication info
# Joining publication info onto the publication specific events
tables_created <- tables_created |>
  mutate(slug = str_remove(pagePath, "^/(find-statistics|data-tables)/")) |>
  mutate(slug = str_remove(slug, "/.*")) |>
  mutate(slug = str_trim(slug, side = "both")) |>
  mutate(slug = str_remove_all(slug, "[^a-zA-Z0-9-]")) |>
  mutate(slug = str_to_lower(slug)) |>
  left_join(scraped_publications, by = c("slug" = "slug")) |>
  rename("publication" = title) |>
  mutate(publication = ifelse(slug == "fast-track", 
                              sub("/.*", "", eventLabel), 
                              publication)) |>
  mutate(publication = ifelse(pagePath == "/data-tables", 
                              sub("/.*", "", eventLabel), 
                              publication)) |>                            
  mutate(publication = str_trim(publication, side = "both")) |>
  mutate(publication = str_to_title(publication))

dates <- create_dates(max(tables_created$date))

test_that("There are no missing dates since we started", {
  expect_equal(
    setdiff(tables_created$date, seq(as.Date(dates$all_time_date), max(dates$latest_date), by = "day")) |>
      length(),
    0
  )
})

# There are some rows (23) that don't have a publication but they look like dodgy pagePaths, leaving in the total but they won't pull through for specific publications

# COMMAND ----------

# selecting just the columns of interest
# TO DO: decide if we only want subsets of page_types in here (e.g make it just about publications or remove defunct pages like data catalogue)

tables_created <- tables_created %>%
  select(date, pagePath, page_type, publication, eventLabel, eventCount)


# COMMAND ----------

# going to aggregate and store at publication level for now, can unpick later if needed 
tables_created <- tables_created %>%
  group_by(date, page_type, publication, eventLabel) %>%
  summarise(
    eventCount = sum(eventCount)
  ) 

# COMMAND ----------

# DBTITLE 1,Write out app data
updated_spark_df <- copy_to(sc, tables_created, overwrite = TRUE)

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
  expect_equal(nrow(temp_table_data), nrow(tables_created))
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
# MAGIC - **page_type**: Type of service page
# MAGIC - **publication**: The associated publication title
# MAGIC - **eventLabel**: The info we have for what subject was used to create the table
# MAGIC - **eventCount**: The number of tables created on given day
# MAGIC
