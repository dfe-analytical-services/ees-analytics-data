# Databricks notebook source
# DBTITLE 1,Load dependencies
source("utils.R")

packages <- c("sparklyr", "DBI", "testthat", "arrow")

install_if_needed(packages)
lapply(packages, library, character.only = TRUE)

table_name <- "catalog_40_copper_statistics_services.analytics_app.ees__last_updated"

sc <- spark_connect(method = "databricks")

# COMMAND ----------

# DBTITLE 1,Record time
create_table_query <- paste("
CREATE TABLE IF NOT EXISTS", table_name, "(
  last_updated TIMESTAMP,
  latest_data DATE
);
")

insert_data_query <- paste("
INSERT OVERWRITE TABLE", table_name, "(last_updated, latest_data)
VALUES (current_timestamp(), current_date() - INTERVAL 2 DAY);
")

dbExecute(sc, create_table_query)
dbExecute(sc, insert_data_query)

# COMMAND ----------

# DBTITLE 1,Check the dates match
max_date_query <- function(table){
  paste0("
    SELECT MAX(date) as max_date
    FROM catalog_40_copper_statistics_services.analytics_app.", table
  )
} 

latest_data <- dbGetQuery(sc, paste("SELECT latest_data FROM", table_name, ""))$latest_data

max_date_service <- dbGetQuery(sc, max_date_query("ees_service"))$max_date
max_date_page <- dbGetQuery(sc, max_date_query("ees_page"))$max_date

# Throw an error (and therefore trigger alert) if any of the dates don't match
test_that("Max date matches latest_data", {
  expect_equal(max_date_service, latest_data)
  expect_equal(max_date_page, latest_data)
})
