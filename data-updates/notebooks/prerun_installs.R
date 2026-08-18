# install packages at beginning of pipeline run
source("utils.R")

install_duckdb()

packages <- c("sparklyr", "DBI", "testthat", "arrow", "duckplyr")

install_if_needed(packages)
lapply(packages, library, character.only = TRUE)
