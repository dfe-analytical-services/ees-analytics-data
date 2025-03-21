# Databricks notebook source
# DBTITLE 1,Load dependencies
source("utils.R")

packages <- c("sparklyr", "DBI", "dplyr", "testthat", "arrow", "stringr")

install_if_needed(packages)
lapply(packages, library, character.only = TRUE)

ga4_event_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_ga4_events"
ua_event_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_ua_events"
scrape_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_pub_scrape"
write_table_name <- "catalog_40_copper_statistics_services.analytics_app.ees_downloads"

sc <- spark_connect(method = "databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC For search events in GA4 we need:
# MAGIC
# MAGIC **eventName**
# MAGIC
# MAGIC - CSV Download Button Clicked (table tool, permalinks)
# MAGIC - Data Set File Download (data catalogue)
# MAGIC - Download All Data Button Clicked (release pages)
# MAGIC - ODS Download Button Clicked (table tool, permalinks)
# MAGIC - Data Set File Download - All (data catalogue)
# MAGIC - Release Page File Downloaded (release ancillary)
# MAGIC - Release Page All Files, Release: Week 8 (and similar across all pubs) (release pages)
# MAGIC - Data Catalogue Page Selected Files Downl
# MAGIC
# MAGIC
# MAGIC **eventLabel** gives the dataset title
# MAGIC
# MAGIC **eventCategory** gives either the page title for where the download event occured on
# MAGIC
# MAGIC NOTE:
# MAGIC We also have tracking for the following eventName's but are conciously not including them here as they only appear in one row each and seem to be in error
# MAGIC
# MAGIC - Release Page All Files downloads.title}, (release pages)
# MAGIC - file_download
# MAGIC
# MAGIC NOTE: 
# MAGIC We don't seem to be tracking any searches on the table tool
# MAGIC
# MAGIC For search events in UA we need: 
# MAGIC
# MAGIC **eventAction**
# MAGIC - Release Page File Downloaded (release pages)
# MAGIC - CSV Download Button Clicked (table tool, permalinks)
# MAGIC - Excel Download Button Clicked (table tool, permalinks)
# MAGIC - ODS Download Button Clicked (table tool, permalinks)
# MAGIC - Data Catalogue Page Selected Files Download (data catalogue)
# MAGIC - Release Page All Files Downloaded (release pages)
# MAGIC - Download All Data Button Clicked (release pages)
# MAGIC - Release Page All Files downloads.title}, Release: Week 8 2023, File: All Files (and similar across all pubs) (release pages)
# MAGIC
# MAGIC **eventLabel** gives the search term used
# MAGIC
# MAGIC **eventCategory** gives either the page title or slug of the page the search event occured on
# MAGIC
# MAGIC NOTE:
# MAGIC We also have tracking for the following eventName's but are conciously not including them here
# MAGIC - Download Latest Data Page All Files Downloaded (old data catalogue)
# MAGIC - Download Latest Data Page File Downloaded (old data catalogue)

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

downloads <- full_data %>% 
  filter(
    eventName %in% c('CSV Download Button Clicked', 'Data Set File Download', 'Download All Data Button Clicked', 'ODS Download Button Clicked', 'Data Set File Download - All', 'Release Page File Downloaded', 'Release Page All Files, Release', 'Data Catalogue Page Selected Files Downl', 'Excel Download Button Clicked', 'Data Catalogue Page Selected Files Download', 'Release Page All Files Downloaded') | 
    str_starts(eventName, "Release Page All Files downloads.title, Release:")
  )


# COMMAND ----------

# DBTITLE 0,Join together and check table integrity
test_that("No duplicate rows", {
  expect_true(nrow(downloads) == nrow(dplyr::distinct(downloads)))
})

test_that("Data has no missing values", {
  expect_false(any(is.na(downloads)))
})

dates <- create_dates(max(downloads$date))

test_that("There are no missing dates since we started", {
  expect_equal(
    setdiff(downloads$date, seq(as.Date(dates$all_time_date), max(dates$latest_date), by = "day")) |>
      length(),
    0
  )
})


# COMMAND ----------

# DBTITLE 1,Adding a page_type column to help distinguish between different types of download
downloads <- downloads %>%
  mutate(page_type = case_when(

    str_detect(eventCategory, "Table Tool") ~ "Table tool",
    
    str_detect(eventCategory, "Data Catalogue") ~ "Data catalogue",
    str_detect(eventCategory, "Data Catalogue - Data Set Page") ~ "Data catalogue",
    str_detect(eventName, "Data Catalogue Page Selected Files Download") ~ "Data catalogue",
    str_detect(eventName, "Data Catalogue Page Selected Files Downl") ~ "Release page",
    
    str_detect(eventCategory, "Permalink Page") ~ "Permalinks",

    str_detect(eventName, "Release Page File Downloaded") ~ "Release page",
    str_detect(eventName, "Release Page All Files Downloaded") ~ "Release page",
    str_detect(eventCategory, "- Useful Information") ~ "Release page",
    str_detect(eventCategory, "Attendance in Education and Early Years Settings During the Coronavirus \\(COVID-19\\) Pandemic Release ") ~ "Release page",
    str_detect(eventCategory, "Outcomes for Children in Need, Including Children Looked After by Local Authorities in England Relea") ~ "Release page",
    str_detect(eventCategory, "Participation in Education, Training and NEET Age 16 to 17 by Local Authority Release Page - Useful ") ~ "Release page",
    str_detect(eventCategory, "Higher Education Entrants and Qualifiers by Their Level 2 and 3 Attainment Release Page - Useful Inf") ~ "Release page",
    str_detect(eventCategory, "September Guarantee: Offers of Education and Training for Young People Age 16 and 17 Release Page - ") ~ "Release page",
    str_detect(eventCategory, "UK Revenue From Education Related Exports and Transnational Education Activity Release Page - Useful") ~ "Release page",
    str_detect(eventCategory, "Foundation Year Participation, Provision and Outcomes at HE Providers Release Page - Useful Informat") ~ "Release page",
    str_detect(eventCategory, "FE Learners Going Into Employment and Learning Destinations by Local Authority District Release Page") ~ "Release page",
    str_detect(eventCategory, "Expansion to Early Childcare Entitlements: Eligibility Codes Issued and Validated Release Page - Use") ~ "Release page",
    str_detect(eventCategory, "Looked After Children Aged 16 to 17 in Independent or Semi-Independent Placements Release Page - Use") ~ "Release page",
    str_detect(eventCategory, "Education, Children’s Social Care and Offending: Local Authority Level Dashboard Release Page - Usef") ~ "Release page",
    str_detect(eventCategory, "Children's Social Work Workforce: Attrition, Caseload, and Agency Workforce Release Page - Useful In") ~ "Release page",

    TRUE ~ 'NA'
  ))


# COMMAND ----------

test_that("There are no events without a page type classification", {
    expect_true(nrow(downloads %>% filter(page_type =='NA')) == 0)
    })

# COMMAND ----------

# DBTITLE 1,Adding a download_type column to help distinguish between different types of download
downloads <- downloads %>%
  mutate(download_type = case_when(

    str_detect(eventName, "ODS Download Button Clicked") ~ "ODS",
    
    str_detect(eventName, "CSV Download Button Clicked") ~ "CSV",

    str_detect(eventName, "Excel Download Button Clicked") ~ "Excel",
    
    str_detect(eventName, "Download All Data Button Clicked") ~ "All files",
    str_detect(eventName, "Release Page All Files Downloaded") ~ "All files",
    str_starts(eventName, "Release Page All Files downloads.title") ~ "All files",
    str_detect(eventName, "Data Set File Download - All") ~ "All files",

    str_detect(eventName, "Data Set File Download") ~ "Data catalogue",
    str_detect(eventName, "Data Catalogue Page Selected Files Download") ~ "Data catalogue",
    str_detect(eventName, "Data Catalogue Page Selected Files Downl") ~ "Data catalogue",

    str_detect(eventName, "Release Page File Downloaded") ~ "Ancillary",

    TRUE ~ 'NA'
  ))


# COMMAND ----------

# DBTITLE 1,Tests
test_that("There are no events without a page type classification", {
    expect_true(nrow(downloads %>% filter(download_type =='NA')) == 0)
    })

# COMMAND ----------

# DBTITLE 1,Bringing in scraped publications list
scraped_publications <- sparklyr::sdf_sql(sc, paste("SELECT * FROM", scrape_table_name)) |> collect()

slugs <- unique(scraped_publications$slug)

# COMMAND ----------

# DBTITLE 1,Joining publication info
# Joining publication info onto the publication specific events
downloads <- downloads |>
  mutate(slug = str_remove(pagePath, "^/(find-statistics|data-tables|data-catalogue)/")) |>
  mutate(slug = str_remove(slug, "/.*")) |>
  mutate(slug = str_trim(slug, side = "both")) |>
  mutate(slug = str_remove_all(slug, "[^a-zA-Z0-9-]")) |>
  mutate(slug = str_to_lower(slug)) |>
  left_join(scraped_publications, by = c("slug" = "slug")) |>
  rename("publication" = title)

dates <- create_dates(max(downloads$date))

test_that("There are no missing dates since we started", {
  expect_equal(
    setdiff(downloads$date, seq(as.Date(dates$all_time_date), max(dates$latest_date), by = "day")) |>
      length(),
    0
  )
})

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from
# MAGIC catalog_40_copper_statistics_services.analytics_app.ees_downloads
# MAGIC where publication is null

# COMMAND ----------

# selecting just the columns we're interested in storing
# TO DO: decide if we only want subsets of page_types in here (e.g make it just about publications or remove defunct pages like data catalogue)

downloads <- downloads %>%
select(date, pagePath, page_type, download_type, publication, eventLabel, eventCount)

# COMMAND ----------

# DBTITLE 1,Write out app data
updated_spark_df <- copy_to(sc, downloads, overwrite = TRUE)

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
  expect_equal(temp_table_data, downloads)
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
# MAGIC - **page_type**: Type of service page (Release page, Data catalogue, permalink etc)
# MAGIC - **download_type**: Type of download (csv, ods, all files etc)
# MAGIC - **publication**: The publication title (relevant for pages that have an associated publication in their pagePath only)
# MAGIC TO DO: for some events we can take publicaiton details from the eventLabel and for permalinks we may be able to get publication from a scrape or the EES database - not doing anythign with these atm! 
# MAGIC - **eventLabel**: The info we have for what file was downloaded (this often includes the relevant publication too though is truncated unhelpfully)
# MAGIC - **eventCount**: The number of downloads of that type on given day
# MAGIC
