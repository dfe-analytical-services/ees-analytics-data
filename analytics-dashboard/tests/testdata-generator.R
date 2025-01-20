# Quick script to make the test files

# Load source data ============================================================
# 1. Use the global and R/load_data files to load all the data in

# Trim data ===================================================================
datasets <- list(
  "joined_data" = joined_data1,
  "publication_aggregation" = pub_agg1,
  "combined_data" = combined_data1
)

# filter above data sets using lapply so the date is from the 1st august to the 8th august
datasets <- lapply(datasets, function(x) {
  x %>%
    filter(date >= "2024-08-01" & date <= "2024-08-08")
})

# Save test data ==============================================================
lapply(names(datasets), function(name) {
  arrow::write_dataset(
    datasets[[name]],
    "analytics-dashboard/tests/testdata/",
    format = "parquet",
    basename_template = paste0(name, "_{i}.parquet")
  )
})

# Write scrape spine out separate for now
arrow::write_dataset(
  pubs1,
  "analytics-dashboard/tests/testdata/",
  format = "parquet",
  basename_template = "pub_spine_{i}.parquet"
)
