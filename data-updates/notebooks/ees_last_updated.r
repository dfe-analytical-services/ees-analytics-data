# Databricks notebook source
# DBTITLE 1,Load dependencies
source("utils.R")

install.packages(
  c("sparklyr", "DBI", "testthat"), 
  repos = repo_url
)

library(sparklyr)
library(DBI)
library(testthat)

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
max_date_query <- "
SELECT MAX(date) as max_date
FROM catalog_40_copper_statistics_services.analytics_app.ees_total
"

latest_data_query <- paste("
SELECT latest_data
FROM", table_name, "
")

max_date <- dbGetQuery(sc, max_date_query)$max_date
latest_data <- dbGetQuery(sc, latest_data_query)$latest_data

# Throw an error (and therefore trigger alert) if the dates don't match
test_that("Max date matches latest_data", {
  expect_equal(max_date, latest_data)
})
