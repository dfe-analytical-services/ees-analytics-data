# Packages --------------------------------------------------------------------

library(googleAnalyticsR)
library(googleAuthR)

# API calls -------------------------------------------------------------------

find_stats_searches <- ga_data(
  369420610,
  metrics = c("eventCount"),
  dimensions = c("date", "pagePath", "eventName", "customEvent:event_label", "customEvent:event_category"),
  dim_filters = ga_data_filter("eventName" == "Publications Filtered by Search"),
  date_range = c("2023-06-02", paste(Sys.Date() - 1)),
  limit = -1
)

release_page_searches <- ga_data(
  369420610,
  metrics = c("eventCount"),
  dimensions = c("date", "pagePath", "eventName", "customEvent:event_label", "customEvent:event_category"),
  dim_filters = ga_data_filter("eventName" == "PageSearchForm"),
  date_range = c("2023-06-02", paste(Sys.Date() - 1)),
  limit = -1
)

data_catalogue_searches <- ga_data(
  369420610,
  metrics = c("eventCount"),
  dimensions = c("date", "pagePath", "eventName", "customEvent:event_label", "customEvent:event_category"),
  dim_filters = ga_data_filter("eventName" == "Data Sets Filtered by searchTerm"),
  date_range = c("2023-06-02", paste(Sys.Date() - 1)),
  limit = -1
)

# Process and save data -------------------------------------------------------

refine_and_save <- function(data, file_name) {
  data <- data |>
    dplyr::rename("search" = `customEvent:event_label`) |>
    dplyr::group_by(search) |>
    dplyr::summarise(eventCount = sum(eventCount)) |>
    dplyr::arrange(desc(eventCount))

  write.csv(data, file_name, row.names = FALSE)
}

refine_and_save(find_stats_searches, "find_stats_searches_2023-06-22_2025-01-22.csv")
refine_and_save(release_page_searches, "release_page_searches_2023-06-22_2025-01-22.csv")
refine_and_save(data_catalogue_searches, "data_catalogue_searches_2023-06-22_2025-01-22.csv")
