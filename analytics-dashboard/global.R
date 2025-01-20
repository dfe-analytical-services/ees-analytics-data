message("Loading dependencies...")
shhh <- suppressPackageStartupMessages # It's a library, so shhh!

# Standard app styling
shhh(library(bslib))
shhh(library(shiny))
shhh(library(bsicons))
shhh(library(shinyWidgets))

# Google analytics
shhh(library(googleAnalyticsR))
shhh(library(googleAuthR))

# Data processing
shhh(library(lubridate))
shhh(library(dplyr))
shhh(library(stringr))
shhh(library(tibble))
shhh(library(data.table))
shhh(library(mgsub))
shhh(library(tidyr))
shhh(library(DT))
shhh(library(snakecase))
shhh(library(janitor))
shhh(library(readr))
shhh(library(anytime))
shhh(library(arrow))
shhh(library(dfeR))

# Scrapey scrapey
shhh(library(rvest))
shhh(library(httr))

# Database connection
shhh(library(dbplyr))
shhh(library(DBI))
shhh(library(config))
shhh(library(odbc))

# Data vis
shhh(library(plotly))

message("...library calls done, setting up global variables...")

# Global variables ============================================================
link_guidance <- tags$a(
  img(
    src = "Fred.png",
    width = "30",
    height = "30"
  ),
  "Guidance",
  href = paste0(
    "https://dfe-analytical-services.github.io/analysts-guide/",
    "statistics-production/user-analytics.html"
  ),
  target = "_blank"
)

link_shiny <- tags$a(
  shiny::icon("github"),
  "Shiny",
  href = "https://github.com/rstudio/shiny", target = "_blank"
)
link_posit <- tags$a(
  shiny::icon("r-project"), "Posit",
  href = "https://posit.co", target = "_blank"
)

latest_date <- Sys.Date() - 1
week_date <- latest_date - 7
four_week_date <- latest_date - 28
since_4thsep_date <- "2024-09-02"
six_month_date <- latest_date - 183
one_year_date <- latest_date - 365
all_time_date <- "2020-04-03"

# Custom functions ============================================================
source("R/utils.R")

message("...global variables set, loading data...")

# Load in data ================================================================

# File paths are relative to analytics-dashboard/ directory

if (Sys.getenv("TEST_MODE") == "") {
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

  message("...connected to database...")

  joined_data1 <- tbl(connection, "ees_analytics_page_data") %>%
    as.data.frame()

  message("...page data loaded, loading publication aggregations...")

  pub_agg1 <- tbl(connection, "ees_analytics_publication_agg") %>%
    as.data.frame()

  message("...publication aggregations loaded, loading service data...")

  combined_data1 <- tbl(connection, "ees_analytics_service_data") %>%
    as.data.frame()

  message("...service data loaded, loading publication spine...")

  pubs1 <- read_csv("data/pubs.csv", show_col_types = FALSE)

  message("Complete!")
} else if (Sys.getenv("TEST_MODE") == "TRUE") {
  message("...in test mode...")

  joined_data1 <- arrow::read_parquet(
    "tests/testdata/joined_data_0.parquet"
  )

  pub_agg1 <- arrow::read_parquet(
    "tests/testdata/publication_aggregation_0.parquet"
  )
  combined_data1 <- arrow::read_parquet(
    "tests/testdata/combined_data_0.parquet"
  )

  pubs1 <- arrow::read_parquet(
    "tests/testdata/pub_spine_0.parquet"
  )

  message("Complete!")
} else {
  message("...no data loaded. TEST_MODE = ", Sys.getenv("TEST_MODE"))
}
