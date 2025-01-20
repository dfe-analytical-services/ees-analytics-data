message("Loading dependencies...")
library(bslib)
library(shiny)
shhh <- suppressPackageStartupMessages # It's a library, so shhh!
shhh(library(googleAnalyticsR))
shhh(library(googleAuthR))
shhh(library(lubridate))
shhh(library(dplyr))
shhh(library(stringr))
shhh(library(tibble))
shhh(library(data.table))
shhh(library(mgsub))
shhh(library(tidyr))
shhh(library(rvest))
shhh(library(httr))
shhh(library(dbplyr))
shhh(library(DBI))
shhh(library(config))
shhh(library(shiny))
shhh(library(DT))
shhh(library(snakecase))
shhh(library(janitor))
shhh(library(rvest))
shhh(library(readr))
shhh(library(anytime))
shhh(library(odbc))

shhh(library(testthat))
shhh(library(plotly))
shhh(library(shinytest))
shhh(library(styler))
shhh(library(bsicons))
shhh(library(shinyWidgets))

message("...library calls done, connecting to database...")


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

message("...connected to database, setting up global variables...")

link_guidance <- tags$a(img(src = "Fred.png", width = "30", height = "30"), "Guidance", href = "https://dfe-analytical-services.github.io/analysts-guide/statistics-production/user-analytics.html", target = "_blank")

link_shiny <- tags$a(shiny::icon("github"), "Shiny", href = "https://github.com/rstudio/shiny", target = "_blank")
link_posit <- tags$a(shiny::icon("r-project"), "Posit", href = "https://posit.co", target = "_blank")

latest_date <- Sys.Date() - 1
week_date <- latest_date - 7
four_week_date <- latest_date - 28
since_4thsep_date <- "2024-09-02"
six_month_date <- latest_date - 183
one_year_date <- latest_date - 365
all_time_date <- "2020-04-03"

filter_on_date <- function(data, period) {
  first_date <- if (period == "week") {
    week_date
  } else if (period == "four_week") {
    four_week_date
  } else if (period == "since_2ndsep") {
    since_4thsep_date
  } else if (period == "six_month") {
    six_month_date
  } else if (period == "one_year") {
    one_year_date
  } else if (period == "all_time") {
    all_time_date
  } else {
    "2020-04-03"
  }

  data %>% filter(date >= first_date & date <= latest_date)
}

filter_on_date_pub <- function(data, period, page) {
  first_date <- if (period == "week") {
    week_date
  } else if (period == "four_week") {
    four_week_date
  } else if (period == "since_4thsep") {
    since_4thsep_date
  } else if (period == "six_month") {
    six_month_date
  } else if (period == "one_year") {
    one_year_date
  } else if (period == "all_time") {
    all_time_date
  } else {
    "2020-04-03"
  }

  data %>%
    filter(date >= first_date & date <= latest_date) %>%
    filter(publication == page)
}

cs_num <- function(x) {
  format(x, big.mark = ",", trim = TRUE)
}

message("...global variables set up, loading data...")

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
