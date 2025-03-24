# Databricks notebook source
# MAGIC %md
# MAGIC This notebook scrapes EES to find the latest release for each publication that we have in our publication spine and calculates an average read time for each page.
# MAGIC
# MAGIC TODO: Currently we don't count datablock tables or charts, we should add them in once we have access to releases in the EES database and then add 12 seconds for each table or chart present in a page.

# COMMAND ----------

# DBTITLE 1,Load dependencies
source("utils.R")

packages <- c("sparklyr", "DBI", "dplyr", "testthat", "arrow", "stringr", "httr2" , "rvest", "purrr")

install_if_needed(packages)
lapply(packages, library, character.only = TRUE)

scrape_table_name <- "catalog_40_copper_statistics_services.analytics_raw.ees_pub_scrape"
write_table_name <- "catalog_40_copper_statistics_services.analytics_app.ees_avg_readtime"

sc <- spark_connect(method = "databricks")

# COMMAND ----------

# DBTITLE 1,Pull in expected slugs
raw_publication_scrape <- sparklyr::sdf_sql(sc, paste("SELECT slug, title FROM ", scrape_table_name)) %>% 
  collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Average reading time
# MAGIC
# MAGIC I've followed the methodology used in [readtime (PyPi)](https://pypi.org/project/readtime/), which is based off of [Medium's time to read formula](https://help.medium.com/hc/en-us/articles/214991667-Read-time).
# MAGIC
# MAGIC Formula used for read time in seconds:
# MAGIC
# MAGIC > num_words / 265 * 60 + 12 * num_images
# MAGIC
# MAGIC This gives 12 seconds per image and num_images is:
# MAGIC
# MAGIC > num_images + num_data_blocks + num_charts
# MAGIC
# MAGIC In the service each page can have a number of 'data blocks' that can either be just a table, or a chart and a table. We're treating each chart and table as a separate image for the purposes of measuring.
# MAGIC
# MAGIC While readtime suggests that the image duration drops down from 12 seconds by 1 second with each additional image, to a minimum of 3 seconds, I've decided to keep the weighting at 12 seconds as the 'images' in this case are all charts and tables that are information dense and will need at least 12 seconds to interpret. If anything we may be underestimating here.
# MAGIC
# MAGIC This doesn't account for the following, so may underestimate the time taken to fully read:
# MAGIC
# MAGIC - Pop up modals with explanations
# MAGIC - Complexity of charts and tables
# MAGIC
# MAGIC In addition, we don't currently haven't found a way to scrape the number of data block charts and tables, so that's a manual addition if anyone wants to do it for now. Long term we should be able to pull it EES-ily from the EES databases.

# COMMAND ----------

# DBTITLE 1,Function to calculate average read time
get_average_read_time <- function(pub_slug) {
  message("Scraping HTML from ", pub_slug, "...")
  # Scrape in the whole HTML page =============================================
  full_html <- tryCatch(
    read_html(
      paste0(
        "https://explore-education-statistics.service.gov.uk/find-statistics/",
        pub_slug
      )
    ),
    error = function(e) {
      warning(pub_slug, " couldn't be read")
    }
  )

  # If couldn't scrape, just return early with NA =============================
  if (!inherits(full_html, "xml_document")) {
    output <- data.frame(
      slug = pub_slug,
      avg_read_time = NA
    )
    return(output)
  }

  # Calculate words ===========================================================
  content <- lapply(
    c(
      # Text elements
      "p", "h1", "h2", "h3", "h4", "h5", "h6", "li", "a"
    ),
    function(tag) {
      full_html |>
        rvest::html_elements(tag) |>
        rvest::html_text()
    }
  ) |>
    unlist(use.names = FALSE) |>
    paste(collapse = " ")

  word_count <- content |>
    strsplit("\\s+")  |>
    unlist(use.names = FALSE) |>
    length()

  word_read_time_secs <- word_count / 265 * 60

  # Calculate images ===========================================================
  image_count <- full_html |>
    rvest::html_elements("img") |>
    # discount the official stats logo
    purrr::discard(~ rvest::html_attr(.x, "src") == "/assets/images/accredited-official-statistics-logo.svg") |>
    length()
  
  data_block_count <- 0 # TODO: Work out a way to pull from database
  chart_count <- 0 # TODO: Work out a way to pull from database

  image_read_time_secs <- (image_count + data_block_count + chart_count) * 12

  # Return the slug and average read time =====================================
  output <- data.frame(
    slug = pub_slug,
    avg_read_time = word_read_time_secs + image_read_time_secs
  )
  return(output)
}

# COMMAND ----------

# MAGIC %md
# MAGIC We are now scraping the publication generic URLs (e.g. https://explore-education-statistics.service.gov.uk/find-statistics/apprenticeships), as this will automatically redirect us to the latest version of that release. We only care about the slugs we already know about the in the raw scrape as that is what the rest of the analytics is based off.

# COMMAND ----------

# DBTITLE 1,Do the scrape to calculate page times
avg_read_time_table <- do.call(rbind, lapply(unique(raw_publication_scrape$slug), get_average_read_time)) |>
  left_join(raw_publication_scrape, by = "slug")

# COMMAND ----------

# DBTITLE 1,Check we have data for all expected publications
# Get count of publication titles
count_raw_pub_titles <- raw_publication_scrape$title |>
  unique() |>
  length()

successful_scrapes <- avg_read_time_table |>
  filter(!is.na(avg_read_time) & is.numeric(avg_read_time)) |>
  nrow()

testthat::test_that("We have equal numbers of scrape pages to titles", {
  expect_equal(count_raw_pub_titles, count_raw_pub_titles)
})
testthat::test_that("Check final table for duplicate row", {
  expect_equal(nrow(avg_read_time_table), nrow(dplyr::distinct(avg_read_time_table)))
})

testthat::test_that("All publication titles from the original scrape have a row in the new table", {
  original_titles <- unique(raw_publication_scrape$title)
  new_table_titles <- unique(avg_read_time_table$title)
  expect_true(all(original_titles %in% new_table_titles), 
               info = paste("Missing titles:", 
                            toString(original_titles[!original_titles %in% new_table_titles])))
})

# COMMAND ----------

# DBTITLE 1,Write out data
updated_spark_df <- copy_to(sc, avg_read_time_table, overwrite = TRUE)

# Write to temp table while we confirm we're good to overwrite data
spark_write_table(updated_spark_df, paste0(write_table_name, "_temp"), mode = "overwrite")

temp_table_data <- sparklyr::sdf_sql(sc, paste0("SELECT * FROM ", write_table_name, "_temp")) %>% collect()
previous_data <- tryCatch(
  {
    sparklyr::sdf_sql(sc, paste0("SELECT * FROM ", write_table_name)) %>% collect()
  },
  error = function(e) {
    NULL
  }
)

test_that("Temp table data matches updated data", {
  expect_equal(nrow(temp_table_data), nrow(avg_read_time_table))
})

# Replace the old table with the new one
dbExecute(sc, paste0("DROP TABLE IF EXISTS ", write_table_name))
dbExecute(sc, paste0("ALTER TABLE ", write_table_name, "_temp RENAME TO ", write_table_name))

print_changes_summary(temp_table_data, previous_data)
