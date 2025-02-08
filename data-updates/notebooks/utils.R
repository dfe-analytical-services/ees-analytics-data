# Key variables consistent across notebooks
auth_path <- "/Volumes/catalog_40_copper_statistics_services/analytics_raw/auth/ees-analytics-c5875719e665.json"
repo_url <- "https://packagemanager.posit.co/cran/2025-02-07" # freezing to initial date of creation

create_dates <- function(latest_date) {
  list(
    latest_date = latest_date,
    week_date = latest_date - 7,
    four_week_date = latest_date - 28,
    since_4thsep_date = "2024-09-02",
    six_month_date = latest_date - 183,
    one_year_date = latest_date - 365,
    ga4_date = "2023-06-22",
    all_time_date = "2020-04-03"
  )
}

print_changes_summary <- function(new_table, old_table) {
  new_dates <- setdiff(as.character(new_table$date), as.character(old_table$date))
  new_rows <- nrow(as.data.frame(new_table)) - nrow(as.data.frame(old_table))

  message("Updated table summary...")
  message("New rows: ", new_rows)
  message("New dates: ", paste(new_dates, collapse = ","))
  message("Total rows: ", nrow(new_table), " rows")
  message("Column names: ", paste(names(new_table), collapse = ", "))
}
