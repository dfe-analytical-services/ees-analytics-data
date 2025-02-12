# Databricks notebook source
source("../data-updates/notebooks/utils.R")

packages <- c(
  "googleAnalyticsR",
  "googleAuthR",
  "sparklyr",
  "DBI",
  "dplyr",
  "tidyr",
  "testthat",
  "lubridate",
  "arrow"
)

install_if_needed(packages)
lapply(packages, library, character.only = TRUE)

ga_auth(json = auth_path)

# COMMAND ----------

GA4_content_acc_events <- ga_data(
  369420610,
  metrics = c("eventCount"),
  dimensions = c("date", "pagePath", "eventName", "customEvent:event_label", "customEvent:event_category"),
  dim_filters = ga_data_filter("eventName" == "Content Accordion Opened"),
  date_range = c("2024-06-20", "2025-02-11"),
  limit = -1
)

GA4_content_acc_events_filtered <- GA4_content_acc_events %>%
  filter(grepl("leo-graduate-and-postgraduate-outcomes", pagePath))

write.csv(GA4_content_acc_events_filtered, "GA4_content_acc_events_filtered.csv", row.names = FALSE)
