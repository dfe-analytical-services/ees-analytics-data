# Packages --------------------------------------------------------------------

message("Loading dependencies...")
shhh <- suppressPackageStartupMessages # It's a library, so shhh!
shhh(library(googleAnalyticsR))
shhh(library(googleAuthR))
shhh(library(dplyr))
shhh(library(stringr))
shhh(library(tibble))
shhh(library(tidyr))
shhh(library(httr))
shhh(library(dbplyr))
shhh(library(DBI))
shhh(library(config))
shhh(library(readr))
shhh(library(odbc))
message("Complete!")
message("")

# Database connection ---------------------------------------------------------

message("Pulling in database connection details...")
config <- config::get("db_connection")

connection <- dbConnect(odbc::odbc(),
  Driver = config$driver,
  Server = config$server,
  Database = config$database,
  UID = config$uid,
  PWD = config$pwd,
  Trusted_Connection = config$trusted,
  encoding = "UTF-8"
)
message("Complete!")
message("")

# Overall service data  -------------------------------------------------------

# Universal Analytics data from SQL

ua_data <- tbl(connection, "GAapi_by_date") %>%
  as.data.frame()

ua_data <- ua_data %>% select(c("date", "pageviews", "sessions"))

ua_data <- ua_data %>%
  mutate(data = "Universal analytics")

# GA4 data using google analytics API

ga4_data <- ga_data(
  369420610,
  metrics = c("screenPageViews", "sessions"),
  dimensions = c("date"),
  date_range = c("2023-06-02", "2024-10-21"),
  limit = -1
)

ga4_data <- ga4_data %>%
  mutate(data = "GA4") %>%
  as.data.frame()

ga4_data <- ga4_data %>% rename(pageviews = screenPageViews) # renamed in move to GA4

# Joining data

combined_data <- rbind(ua_data, ga4_data)

sum(combined_data$pageviews)
sum(combined_data$sessions)

# By page stuff ---------------------------------------------------------------
## Universal analytics --------------------------------------------------------
ua_page_data <- tbl(connection, "GAapi_pagePath") %>%
  as.data.frame()

ua_page_data <- ua_page_data %>%
  mutate(data = "Universal analytics")

## GA4 data -------------------------------------------------------------------

ga4_page_data <- ga_data(
  369420610,
  metrics = c(
    "totalUsers",
    "activeUsers",
    "screenPageViews",
    "sessions",
    "averageSessionDuration"
  ),
  dimensions = c("date", "pagePath"),
  date_range = c("2023-06-02", "2024-10-21"),
  limit = -1
)

ga4_page_data <- ga4_page_data %>%
  mutate(data = "GA4") %>%
  as.data.frame()

ga4_page_data <- ga4_page_data %>% rename(pageviews = screenPageViews) # renamed in move to GA4

## Combine data ---------------------------------------------------------------

combined_page_data <- rbind(
  select(ua_page_data, c("date", "pagePath", "pageviews", "sessions")),
  select(ga4_page_data, c("date", "pagePath", "pageviews", "sessions"))
)

sum(combined_page_data$pageviews)

sum(combined_page_data$sessions)

write_csv(combined_page_data, "data/combined_page_data.csv")

## Publication data -----------------------------------------------------------

# GA will count any URL hit which bloats the data with a load of stuff that
# actually doesn't exist
# We used to scrape EES and save a list of all expected URLs though spotted all
# sorts of errors in this approach, and it needs adapting now we can supersede
# publications that need to be fixed, or use an alternative method

## TEMP FUDGE to pull in old URLs (before superseding) ------------------------
# - future scrapes should add to this and not overwrite!
scrape_data <- read_csv("reference-data/scrape_data_fudge.csv")
nrow(scrape_data) # 907

# TODO - fix the expected urls list
# Either scrape it from find stats
# or connect direct to the EES prod database
pubs <- select(scrape_data, publication) %>%
  filter(!str_detect(publication, "methodology")) %>%
  filter(!str_detect(publication, "Methodology")) %>%
  unique()

nrow(pubs) # 106

## Joining page data to scrape data (the spine of pages we expect) ------------

combined_page_data1 <- combined_page_data %>%
  mutate(url = paste0(
    "https://explore-education-statistics.service.gov.uk",
    pagePath
  ))

nrow(combined_page_data1)

sum(combined_page_data1$pageviews)

joined_data <- left_join(
  combined_page_data1,
  scrape_data,
  by = c("url" = "url")
)

nrow(joined_data)

sum(joined_data$pageviews)

## Aggregate for publications -------------------------------------------------

pub_agg <- joined_data %>%
  filter(
    !str_detect(url, "methodology") &
      !str_detect(url, "data-tables") &
      !str_detect(url, "data-guidance") &
      !str_detect(url, "prerelease-access-list")
  ) %>%
  group_by(date, publication) %>%
  summarise("sessions" = sum(sessions), "pageviews" = sum(pageviews))

nrow(pub_agg)

sum(pub_agg$pageviews)

# Write out parquet files for the app to use temporarily ----------------------

# Write data to database ------------------------------------------------------
# write_csv(pubs, "data/pubs.csv")
#
# dbWriteTable(
#   conn = connection,
#   name = Id(
#     schema  = "dbo",
#     table   = "ees_analytics_service_data"
#   ),
#   value = combined_data,
#   overwrite = TRUE
# )
#
#
# dbWriteTable(
#   conn = connection,
#   name = Id(
#     schema  = "dbo",
#     table   = "ees_analytics_page_data"
#   ),
#   value = joined_data,
#   overwrite = TRUE
# )
#
# dbWriteTable(
#   conn = connection,
#   name = Id(
#     schema  = "dbo",
#     table   = "ees_analytics_publication_agg"
#   ),
#   value = pub_agg,
#   overwrite = TRUE
# )
