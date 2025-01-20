# What's interesting on a day

the_day <- "2024-02-01"

base_figs <- filter(combined_data1, date == the_day)
top_pages <- filter(joined_data1, date == the_day) %>% arrange(desc(pageviews))
top_5_pages <- top_pages[1:5, ] %>% select(url, heading, time_period, pageviews)

top_accordions <- filter(event_accordions, date == the_day) %>% arrange(desc(eventCount))
top_5_accordions <- top_accordions[1:5, ] %>% select(url, heading, eventLabel, eventCount)

top_downloads <- filter(event_downloads, date == the_day) %>% arrange(desc(eventCount))
top_5_downloads <- top_downloads[1:5, ] %>% select(url, heading, eventLabel, eventCount)

top_search <- filter(event_search, date == the_day) %>%
  group_by(eventLabel) %>%
  summarise(total = sum(eventCount)) %>%
  arrange(desc(total))

top_5_search <- top_search[1:5, ]


base_figs
top_5_accordions
top_5_downloads
top_5_search
