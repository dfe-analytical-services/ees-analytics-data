find_stats_searches <- googleAnalyticsR::ga_data(
  369420610,
  metrics = c("eventCount"),
  dimensions = c("date", "pagePath", "eventName", "customEvent:event_label", "customEvent:event_category"),
  dim_filters = googleAnalyticsR::ga_data_filter("eventName" == "Publications Filtered by Search"),
  date_range = c("2023-06-02", paste(Sys.Date() - 1)),
  limit = -1
)

# aggregate the data by search term using event_label
searches <- find_stats_searches |>
  dplyr::group_by(`customEvent:event_label`) |>
  dplyr::summarise(searches = sum(eventCount)) |>
  dplyr::arrange(desc(searches))

# print top 10 search terms
print(searches[1:10, ])
