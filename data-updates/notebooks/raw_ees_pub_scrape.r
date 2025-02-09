# Databricks notebook source
source("utils.R")

packages <- c(
  "rvest",
  "sparklyr",
  "DBI",
  "dplyr",
  "tidyr",
  "testthat",
  "arrow"
)

install_if_needed(packages)
lapply(packages, library, character.only = TRUE)

legacy_table <- "catalog_40_copper_statistics_services.analytics_raw.ees_legacy_scrape"
write_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_pub_scrape"

sc <- spark_connect(method = "databricks")

homepage <- "https://explore-education-statistics.service.gov.uk"
find_stats <- paste0(homepage, "/find-statistics/")

# COMMAND ----------

# DBTITLE 1,Pull in legacy scraped data
possible_suffixes <- c("methodology", "data-guidance", "prerelease-access-list")

legacy_scrape_data <- sparklyr::sdf_sql(sc, paste0("SELECT DISTINCT url, heading FROM ", legacy_table, 
    " WHERE url LIKE '%find-statistics%' AND NOT (url LIKE '%", 
    paste(possible_suffixes, collapse = "%' OR url LIKE '%"), "%')")) %>% 
    collect() |>
    mutate(url = gsub(find_stats, "", url)) |>
    mutate(url = sub("/.*", "", url)) |>
    distinct() 

# COMMAND ----------

# DBTITLE 1,Scrape for current publications
total_pages <- extract_total_pages(find_stats)
expected_num_pubs <- extract_total_pubs(find_stats)
find_stats_pages <- paste0(homepage, "/find-statistics?page=", 1:total_pages)

page_slugs <- lapply(find_stats_pages, scrape_publications) |>
  unlist(use.names = FALSE) |>
  unique()

test_that("We've scraped the expected number", {
  expect_equal(length(page_slugs), expected_num_pubs)
})

expected_pages_with_info <- lapply(pub_slugs, get_publication_title)

latest_scrape <- bind_rows(lapply(expected_pages_with_info, as.data.frame)) %>%
  rename(url = V1, heading = V2) |>
  mutate(url = gsub(find_stats, "", url)) |>
  distinct() 

# COMMAND ----------

# DBTITLE 1,Combine with existing slugs
combined_scrape <- rbind(latest_scrape, legacy_scrape_data) |>
  distinct() |>
  filter(heading != "Find statistics and data") |>
  as.data.frame() |>
  rename(slug = url, title = heading)

test_that("Number of rows is more than find stats results", {
  expect_gt(nrow(combined_scrape), expected_num_pubs)
})

# COMMAND ----------

# DBTITLE 1,Write out scraped publications to table
updated_spark_df <- copy_to(sc, combined_scrape, overwrite = TRUE)

# Write to temp table while we confirm we're good to overwrite data
spark_write_table(updated_spark_df, paste0(write_table_name, "_temp"), mode = "overwrite")

temp_table_data <- sparklyr::sdf_sql(sc, paste0("SELECT * FROM ", write_table_name, "_temp")) %>% collect()

test_that("Temp table data matches updated data", {
  expect_equal(nrow(temp_table_data), nrow(combined_scrape))
})

previous_data <- tryCatch({
  sparklyr::sdf_sql(sc, paste0("SELECT * FROM ", write_table_name)) %>% collect()
}, error = function(e) {
  NULL
})

if(!is.null(previous_data)){
  test_that("Number of pub / slug combos is less than 10% more than previous", {
    expect_lt(nrow(temp_table_data), nrow(previous_data) * 1.1)
  })
}

# Replace the old table with the new one
dbExecute(sc, paste0("DROP TABLE IF EXISTS ", write_table_name))
dbExecute(sc, paste0("ALTER TABLE ", write_table_name, "_temp RENAME TO ", write_table_name))
