# Databricks notebook source
# DBTITLE 1,Load dependencies
source("utils.R")

install.packages(
  c(
    "sparklyr",
    "DBI",
    "dplyr",
    "testthat"
  ),
  repos = repo_url
)

library(sparklyr)
library(DBI)

library(dplyr)
library(testthat)

table_name <- "catalog_40_copper_statistics_services.analytics_app.ees__last_updated"

sc <- spark_connect(method = "databricks")

# COMMAND ----------
# DBTITLE 1,Record time

1 + 1
