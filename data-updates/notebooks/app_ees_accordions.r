# Databricks notebook source
# DBTITLE 1,Load dependencies
source("utils.R")

packages <- c("sparklyr", "DBI", "dplyr", "testthat", "arrow", "stringr")

install_if_needed(packages)
lapply(packages, library, character.only = TRUE)

ga4_event_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_ga4_events"
ua_event_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_ua_events"
scrape_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_pub_scrape"
write_table_name <- "catalog_40_copper_statistics_services.analytics_app.ees_accordions"

sc <- spark_connect(method = "databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC For accordion opens in GA4 we need:
# MAGIC
# MAGIC **eventName**
# MAGIC - Content Accordion Opened
# MAGIC - Annexes Accordion Opened
# MAGIC - Accordion Opened
# MAGIC - Data Accordion Opened
# MAGIC - Publications Accordion Opened
# MAGIC
# MAGIC eventLabel gives the relevant accordion name
# MAGIC
# MAGIC eventCategory gives the title of the page the event occured on
# MAGIC
# MAGIC For accordion opens in UA we need: 
# MAGIC
# MAGIC **eventAction**
# MAGIC - Content Accordion Opened
# MAGIC - Accordion Opened
# MAGIC - Annexes Accordion Opened
# MAGIC - Data Accordion Opened
# MAGIC - Publications Accordion Opened
# MAGIC - Publications+Accordion+Opened << this one doesn't exist in GA4
# MAGIC
# MAGIC eventLabel gives the relevant accordion name
# MAGIC
# MAGIC eventCategory gives the title of the page the event occured on

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

accordion_events <- full_data %>% filter(eventName %in% c('Content Accordion Opened', 'Accordion Opened', 'Annexes Accordion Opened', 'Data Accordion Opened', 'Publications Accordion Opened', 'Publications+Accordion+Opened'))

# COMMAND ----------

# DBTITLE 0,Join together and check table integrity
test_that("No duplicate rows", {
  expect_true(nrow(accordion_events) == nrow(dplyr::distinct(accordion_events)))
})

test_that("Data has no missing values", {
  expect_false(any(is.na(accordion_events)))
})

dates <- create_dates(max(accordion_events$date))

test_that("There are no missing dates since we started", {
  expect_equal(
    setdiff(accordion_events$date, seq(as.Date(dates$all_time_date), max(dates$latest_date), by = "day")) |>
      length(),
    0
  )
})


# COMMAND ----------

# DBTITLE 1,Adding a page type grouping
accordion_events <- accordion_events %>%
  mutate(page_type = case_when(
    ## Main recording/groupings for the pages we'll be most interested in
    str_detect(eventCategory, "Release Page") ~ "Release page",
    str_detect(eventCategory, "Methodology") ~ "Methodology",
    str_detect(eventCategory, "Glossary") ~ "Glossary",
    ## Some manual faffery to address the publications that have too long a title
    str_detect(eventCategory, "Attendance in Education and Early Years Settings During the Coronavirus \\(COVID-19\\) Pandemic Release ") ~ "Release page",    
    str_detect(eventCategory, "Attendance in Education and Early Years Settings During the Coronavirus \\(COVID-19\\) Pandemic Methodol") ~ "Methodology",
    str_detect(eventCategory, "Outcomes for Children in Need, Including Children Looked After by Local Authorities in England Metho") ~ "Methodology",
    str_detect(eventCategory, "Outcomes for Children in Need, Including Children Looked After by Local Authorities in England Relea") ~ "Release page", 
    ## Service pages that have / did have accordions, might be helpful to look at but not main focus
    str_detect(eventCategory, "Methodologies") ~ "Methodology navigation",
    str_detect(eventCategory, "Find Statistics and Data") ~ "Find stats navigation",
    str_detect(eventCategory, "Find\\+Statistics\\+and\\+Data") ~ "Find stats navigation",
    str_detect(eventCategory, "Download Index Page") ~ "Old data catalogue",
    TRUE ~ 'NA'
  ))

# COMMAND ----------

# DBTITLE 1,Tests
test_that("There are no events without a page type classification", {
    expect_true(nrow(accordion_events %>% filter(page_type =='NA')) == 0)
    })

# COMMAND ----------

# DBTITLE 1,Bringing in scraped publications list
scraped_publications <- sparklyr::sdf_sql(sc, paste("SELECT * FROM", scrape_table_name)) |> collect()

slugs <- unique(scraped_publications$slug)

# COMMAND ----------

# DBTITLE 1,Joining publication info
# Joining publication info onto the publication specific events
# TO DO: join to methodology pages too
accordion_events <- accordion_events |>
  mutate(slug = str_remove(pagePath, "^/find-statistics/")) |>
  mutate(slug = str_remove(slug, "/.*")) |>
  mutate(slug = str_remove(slug, "\\.$")) |>
  left_join(scraped_publications, by = c("slug" = "slug")) |>
  rename("publication" = title)
 ## select(date, pagePath, publication, pageviews, sessions)

dates <- create_dates(max(accordion_events$date))

test_that("There are no missing dates since we started", {
  expect_equal(
    setdiff(accordion_events$date, seq(as.Date(dates$all_time_date), max(dates$latest_date), by = "day")) |>
      length(),
    0
  )
})

# COMMAND ----------

# selecting jus tthe columns we're interested in storing
# TO DO: decide if we only want subsets of page_types in here (e.g make it just about publications or remove defunct pages like data catalogue)

accordion_events <- accordion_events %>%
select(date, pagePath, page_type, publication, eventLabel, eventCount)

# COMMAND ----------

# DBTITLE 1,Write out app data
updated_spark_df <- copy_to(sc, accordion_events, overwrite = TRUE)

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
  expect_equal(temp_table_data, accordion_events)
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
# MAGIC - **page_type**: Type of service page (Release page, methoodlogy, glossary etc)
# MAGIC - **publication**: The publication title (relevant for 'Release page' page_types only atm, need to update code to add to methodology pages too)
# MAGIC - **eventLabel**: The accordion title
# MAGIC - **eventCount**: The number of accordion click events on given day
# MAGIC
