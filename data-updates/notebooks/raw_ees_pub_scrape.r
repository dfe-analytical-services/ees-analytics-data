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

legacy_scrape_data <- sparklyr::sdf_sql(sc, paste0(
  "SELECT DISTINCT url, heading FROM ", legacy_table,
  " WHERE url LIKE '%find-statistics%' AND NOT (url LIKE '%",
  paste(possible_suffixes, collapse = "%' OR url LIKE '%"), "%')"
)) %>%
  collect() |>
  mutate(url = gsub(find_stats, "", url)) |>
  mutate(url = sub("/.*", "", url)) |>
  distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC TODO: Could we get the publication title from the link text on find stats rather than needing to scrape individual URLs?

# COMMAND ----------

# DBTITLE 1,Scrape for current publications
total_pages <- extract_total_pages(find_stats)
expected_num_pubs <- extract_total_pubs(find_stats)
find_stats_pages <- paste0(homepage, "/find-statistics?page=", 1:total_pages)

pub_slugs <- lapply(find_stats_pages, scrape_publications) |>
  unlist(use.names = FALSE) |>
  unique()

test_that("We've scraped the expected number", {
  expect_equal(length(pub_slugs), expected_num_pubs)
})

expected_pages_with_info <- lapply(pub_slugs, get_publication_title)

latest_scrape <- bind_rows(lapply(expected_pages_with_info, as.data.frame)) %>%
  rename(url = V1, heading = V2) |>
  mutate(url = gsub(find_stats, "", url)) |>
  distinct()

# COMMAND ----------

# DBTITLE 1,Combine with existing slugs
combined_scrape <- rbind(latest_scrape, legacy_scrape_data) |>
  filter(heading != "Find statistics and data") |>
  as.data.frame() |>
  rename(slug = url, title = heading) |>
  mutate(slug = tolower(slug)) |>
  distinct()

test_that("Number of rows is more than find stats results", {
  expect_gt(nrow(combined_scrape), expected_num_pubs)
})

# COMMAND ----------

# MAGIC %md
# MAGIC TODO: Move this next section out once we have a lookup table to rely on for renaming
# MAGIC
# MAGIC Current known renames that appear as duplicates unless handled (i.e. we have more than one title for a given slug):
# MAGIC * Participation in education, training and employment age 16 to 18
# MAGIC   * Formerly: Participation in education and training and employment
# MAGIC * Pupil absence in schools in England
# MAGIC   * Formerly: Pupil absence in schools in England: autumn and spring terms
# MAGIC
# MAGIC Here we'll just remove them as we don't need them, keeping only the newer title, all the current later matching is done on slugs so as long as every slugs has a unique title we're good (a title can match many slugs, that's not a problem).
# MAGIC
# MAGIC If this errors because there's more, you can get a nice table of the offending pages using the code from the failing test:
# MAGIC ```r
# MAGIC combined_scrape |>
# MAGIC     group_by(slug) |>
# MAGIC     filter(n() > 1) |>
# MAGIC     ungroup() |> 
# MAGIC     display()
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Manually adjust for renamed publications
filtered_scrape <- combined_scrape |>
  filter(!(slug == "participation-in-education-and-training-and-employment" & title == "Participation in education and training and employment") &
         !(slug == "pupil-absence-in-schools-in-england" & title == "Pupil absence in schools in England: autumn and spring terms"))

test_that("Each slug has a unique title", {
  duplicate_slugs <- filtered_scrape |>
    group_by(slug) |>
    filter(n() > 1) |>
    ungroup()

  expect_equal(nrow(duplicate_slugs), 0)
})

# COMMAND ----------

# DBTITLE 1,Write out scraped publications to table
updated_spark_df <- copy_to(sc, filtered_scrape, overwrite = TRUE)

# Write to temp table while we confirm we're good to overwrite data
spark_write_table(updated_spark_df, paste0(write_table_name, "_temp"), mode = "overwrite")

temp_table_data <- sparklyr::sdf_sql(sc, paste0("SELECT * FROM ", write_table_name, "_temp")) %>% collect()

test_that("Temp table data matches updated data", {
  expect_equal(nrow(temp_table_data), nrow(filtered_scrape))
})

previous_data <- tryCatch(
  {
    sparklyr::sdf_sql(sc, paste0("SELECT * FROM ", write_table_name)) %>% collect()
  },
  error = function(e) {
    NULL
  }
)

if (!is.null(previous_data)) {
  test_that("Number of pub / slug combos is less than 10% more than previous", {
    expect_lt(nrow(temp_table_data), nrow(previous_data) * 1.1)
  })

  test_that("Number of pub / slug combos is only going up", {
    expect_gte(nrow(temp_table_data), nrow(previous_data))
  })
}

# Replace the old table with the new one
dbExecute(sc, paste0("DROP TABLE IF EXISTS ", write_table_name))
dbExecute(sc, paste0("ALTER TABLE ", write_table_name, "_temp RENAME TO ", write_table_name))
