# freezing to initial date of creation for fixing dependency versions
# focal seems to be the ubuntu that my current cluster is running (runtime 15.4)
options(repos = c(CRAN = "https://packagemanager.posit.co/cran/__linux__/focal/2025-02-09"))

# Function to use pak to install packages that aren't already installed
install_if_needed <- function(pkg){
if (!requireNamespace("pak", quietly = TRUE)) {
  install.packages("pak")
}

## Handle vectors
if (length(pkg) == 1) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    pak::pkg_install(pkg, ask = FALSE)
  } else {
    message("Skipping install... ", pkg, " already installed")
  }
} else {
  to_install <- pkg[!sapply(pkg, requireNamespace, quietly = TRUE)]
  if (length(to_install) > 0) {
    pak::pkg_install(to_install, ask = FALSE)
  } else {
    message("Skipping install... all packages already installed")
  }
}
}

auth_path <- "/Volumes/catalog_40_copper_statistics_services/analytics_raw/auth/ees-analytics-c5875719e665.json"

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
  if(is.null(old_table)){
    message("New table summary...")
    message("Number of rows: ", nrow(new_table))
    message("Column names: ", paste(names(new_table), collapse = ", "))
  } else {
    new_dates <- setdiff(as.character(new_table$date), as.character(old_table$date))
    new_rows <- nrow(as.data.frame(new_table)) - nrow(as.data.frame(old_table))

    message("Updated table summary...")
    message("New rows: ", new_rows)
    message("New dates: ", paste(new_dates, collapse = ","))
    message("Total rows: ", nrow(new_table), " rows")
    message("Column names: ", paste(names(new_table), collapse = ", "))
  }
}
