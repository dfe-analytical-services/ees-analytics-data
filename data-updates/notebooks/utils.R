# Key variables consistent across notebooks
auth_path <- "/Volumes/catalog_40_copper_statistics_services/analytics_raw/auth/ees-analytics-c5875719e665.json"
repo_url <- "https://packagemanager.posit.co/cran/2025-02-07" # freezing to initial date of creation

create_dates <- function(latest_date){
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
