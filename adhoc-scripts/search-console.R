library(searchConsoleR)

download_dimensions <- c("date", "query", "page", "device", "country")
scr_auth()

searchConsole <- search_analytics(
  siteURL = "https://explore-education-statistics.service.gov.uk/",
  startDate = Sys.Date() - 30,
  endDate = Sys.Date() - 26,
  dimensions = download_dimensions,
  searchType = "web",
  walk_data = "byBatch",
  rowLimit = 200000
)

write.csv(searchConsole, "adhoc-scripts/adhoc-data/christmasgooglesearches.csv", row.names = FALSE)
