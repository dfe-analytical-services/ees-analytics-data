library(searchConsoleR)

download_dimensions <- c("date", "query", "page", "device", "country")
scr_auth()

searchConsole <- search_analytics(
  siteURL = ga_variables$websiteUrl,
  startDate = API_date_old - 2,
  endDate = Sys.Date() - 3,
  dimensions = download_dimensions,
  searchType = "web",
  walk_data = "byBatch",
  rowLimit = 200000
)
