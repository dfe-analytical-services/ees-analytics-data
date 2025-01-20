## Key stage 4 accordions

View(GA4_content_acc_events %>%
  filter(grepl("key-stage-4", pagePath)) %>%
  group_by(`customEvent:event_label`, `customEvent:event_category`) %>%
  summarise(total_eventCount = sum(eventCount, na.rm = TRUE)))

## key stage 4 page views
View(GA4_page_data %>%
  filter(grepl("key-stage-4", pagePath)) %>%
  group_by(pagePath) %>%
  summarise(
    total_pageviews = sum(pageviews, na.rm = TRUE),
    total_sessions = sum(sessions, na.rm = TRUE)
  ))


View(GA4_page_data %>%
  filter(grepl("key-stage-4-performance", pagePath)) %>%
  group_by(date) %>%
  summarise(
    total_pageviews = sum(pageviews, na.rm = TRUE),
    total_sessions = sum(sessions, na.rm = TRUE)
  ))



## Downloads

View(GA4_download_events %>%
  filter(grepl("data-catalogue", pagePath)) %>%
  group_by(date, pagePath, eventName, `customEvent:event_label`, `customEvent:event_category`) %>%
  summarise(total_eventCount = sum(eventCount, na.rm = TRUE)))


## Table events

View(GA4_table_events %>%
  filter(grepl("key-stage-4", pagePath)) %>%
  group_by(eventName, `customEvent:event_label`, `customEvent:event_category`) %>%
  summarise(total_eventCount = sum(eventCount, na.rm = TRUE)))

## Search events

View(GA4_search_events %>%
  filter(grepl("key-stage-4", pagePath)) %>%
  group_by(eventName, `customEvent:event_label`) %>%
  summarise(total_eventCount = sum(eventCount, na.rm = TRUE)))


## download check

events_post_catalogue <- ga_data(
  369420610,
  metrics = c("eventCount"),
  dimensions = c("pagePath", "eventName", "customEvent:event_label", "customEvent:event_category"),
  date_range = c("2024-06-30", "2024-09-09"),
  limit = -1
)

View(
  events_post_catalogue %>%
    # filter(grepl('data-catalogue', pagePath)) %>%
    filter(grepl("key-stage-4-performance", pagePath))
)
