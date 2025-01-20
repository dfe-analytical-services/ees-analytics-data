# TODO: tidy this up and move into app for events data and permalinks data (to recreate all data needed for dashbaord functionality)

# Will probably want adding into creating-basic-analytics-tables.R at some point

# I've made a start but not done any data cleaning or QA

## Events

# Accordion opens ----
# Teams need to see everything relating to their publication, though complicated by splitting for individual releases

UA_content_acc_events <- tbl(connection, "GAapi_events") %>%
  filter(eventAction == "Content Accordion Opened") %>%
  as.data.frame()

UA_content_acc_events <- UA_content_acc_events %>%
  mutate(data = "Universal analytics") %>%
  mutate(url = paste0("https://explore-education-statistics.service.gov.uk", pagePath))

# rename to match GA4 colnames
UA_content_acc_events <- UA_content_acc_events %>% rename(eventCount = totalEvents)
UA_content_acc_events <- UA_content_acc_events %>% rename(eventName = eventAction)

GA4_content_acc_events <- ga_data(
  369420610,
  metrics = c("eventCount"),
  dimensions = c("date", "pagePath", "eventName", "customEvent:event_label", "customEvent:event_category"),
  dim_filters = ga_data_filter("eventName" == "Content Accordion Opened"),
  date_range = c("2023-06-02", "2024-09-09"),
  limit = -1
)

GA4_content_acc_events <- GA4_content_acc_events %>%
  mutate(data = "GA4") %>%
  mutate(url = paste0("https://explore-education-statistics.service.gov.uk", pagePath))



# tidy col names
GA4_content_acc_events <- GA4_content_acc_events %>% rename(eventLabel = "customEvent:event_label") # changed in move to GA4
GA4_content_acc_events <- GA4_content_acc_events %>% rename(eventCategory = "customEvent:event_category") # changed in move to GA4

# Combine data

combined_content_acc_events <-
  rbind(
    select(
      UA_content_acc_events,
      c("data", "date", "pagePath", "url", "eventName", "eventLabel", "eventCategory", "eventCount")
    ),
    select(
      GA4_content_acc_events,
      c("data", "date", "pagePath", "url", "eventName", "eventLabel", "eventCategory", "eventCount")
    )
  )

# checks
nrow(combined_content_acc_events) # 579606
sum(combined_content_acc_events$eventCount) # 2478389

nrow(UA_content_acc_events) + nrow(GA4_content_acc_events) # 579606
sum(UA_content_acc_events$eventCount) + sum(GA4_content_acc_events$eventCount) # 2478389

# join publication info
combined_content_acc_events_joined <- left_join(
  combined_content_acc_events,
  scrape_data,
  by = c("url" = "url") # ,
)

# checks
nrow(combined_content_acc_events_joined) # 579606
sum(combined_content_acc_events_joined$eventCount) # 2478389

# write file
# write_csv(filter(combined_content_acc_events_joined, publication != "NA"), "data/GA4_content_acc_events.csv")
#
# sum(filter(combined_content_acc_events_joined, publication != "NA")$eventCount) #2477669





# Downloads ----

# TO DO - we need to clean and separate this data to give useful info
# For example, all file downloads vs individual files, downloads form catalogue vs release page vs table tool etc
# Needs to join with publication file to let teams see everythign relating to their publication, though complicated by splitting for individual releases

UA_download_events <- tbl(connection, "GAapi_events") %>%
  filter(eventAction == "Download All Data Button Clicked" | eventCategory == "Downloads") %>%
  as.data.frame()

UA_download_events <- UA_download_events %>%
  mutate(data = "Universal analytics") %>%
  mutate(url = paste0("https://explore-education-statistics.service.gov.uk", pagePath))

# rename to match GA4 colnames
UA_download_events <- UA_download_events %>% rename(eventCount = totalEvents)
UA_download_events <- UA_download_events %>% rename(eventName = eventAction)

GA4_download_events <- ga_data(
  369420610,
  metrics = c("eventCount"),
  dimensions = c("date", "pagePath", "eventName", "customEvent:event_label", "customEvent:event_category"),
  dim_filters = ga_data_filter("eventName" == "Download All Data Button Clicked" | "customEvent:event_category" == "Downloads"),
  date_range = c("2023-06-02", "2024-09-09"),
  limit = -1
)

GA4_download_events <- GA4_download_events %>%
  mutate(data = "GA4") %>%
  mutate(url = paste0("https://explore-education-statistics.service.gov.uk", pagePath))

# tidy col names
GA4_download_events <- GA4_download_events %>% rename(eventLabel = "customEvent:event_label")
GA4_download_events <- GA4_download_events %>% rename(eventCategory = "customEvent:event_category")

# #Combine data

combined_download_events <-
  rbind(
    select(
      UA_download_events,
      c("data", "date", "pagePath", "url", "eventName", "eventLabel", "eventCategory", "eventCount")
    ),
    select(
      GA4_download_events,
      c("data", "date", "pagePath", "url", "eventName", "eventLabel", "eventCategory", "eventCount")
    )
  )

# # checks
nrow(combined_download_events) # 218665
sum(combined_download_events$eventCount) # 535510

nrow(UA_download_events) + nrow(GA4_download_events) # 218665
sum(UA_download_events$eventCount) + sum(GA4_download_events$eventCount) # 535510

# #join publication info
combined_download_events_joined <- left_join(
  combined_download_events,
  scrape_data,
  by = c("url" = "url") # ,
)

# # checks
nrow(combined_download_events_joined) # 218665
sum(combined_download_events_joined$eventCount) # 535510

# # write file
# write_csv(combined_download_events_joined, "data/GA4_download_events.csv")




# old data downloads cleaning code ----
# This is very messy and needs refactoring, built up and frankensteined over time and we aren't confident it's doing what it intends to correctly. Take this opportunity to do it a different way!

# expected_release_pages <- filter(scrape_data, time_period != "") %>% select(-is_latest_release)
#
#
# expected_release_pages_unspecified <- expected_release_pages %>%
#   select(publication, release, time_period, last_edited) %>%
#   distinct()
#
# test <- head(combined_download_events_joined)
#
# test %>% mutate(eventItem = basename(str_sub(eventLabel, start = -200)))
#
# release_events <- base::merge(combined_download_events, expected_release_pages, by.x = c("url"), by.y = c("url")) %>%
#   mutate(eventType = case_when(
#     eventName %in% c("pageSearchForm", "PageSearchForm") ~ "Search",
#     grepl("File Downloaded", eventName, ignore.case = TRUE) ~ "File Downloaded",
#     eventCategory %in% "Page Print" ~ "Page Print",
#     TRUE ~ "Other"
#   )) %>%
#   mutate(eventItem = ifelse(eventType == "File Downloaded", basename(str_sub(eventLabel, start = -259)), eventLabel))
#
# release_events_fix <- release_events %>%
#   filter(date < anytime(last_edited)) %>%
#   base::merge(expected_release_pages_unspecified, by = "publication", all.x = TRUE) %>%
#   filter(date >= anytime(last_edited.y)) %>%
#   group_by(publication, url, date, pagePath, eventName, eventLabel, eventCategory) %>%
#   filter(last_edited.y == max(last_edited.y)) %>%
#   select(url, date, pagePath, eventName, eventLabel, eventCategory, eventCount, publication,
#          publication,
#          release = release.y,
#          time_period = time_period.y,
#          last_edited = last_edited.y
#   )
#
# release_events_final <- release_events %>%
#   filter(date > anytime(last_edited)) %>%
#   bind_rows(release_events_fix)
#
#
# CHECK
#
# sum(release_events_fix$eventCount) #535510
#
# other_events <- eventGA %>%
#   filter(!(paste0("https://explore-education-statistics.service.gov.uk", pagePath) %in% expected_release_pages$url)) %>%
#   mutate(url = paste0("https://explore-education-statistics.service.gov.uk", pagePath)) %>%
#   mutate(eventType = case_when(
#     eventAction %in% c("pageSearchForm") ~ "Search",
#     grepl("File Downloaded", eventAction, ignore.case = TRUE) ~ "File Downloaded",
#     eventCategory %in% "Page Print" ~ "Page Print",
#     TRUE ~ "Other"
#   )) %>%
#   mutate(eventItem = ifelse(eventType == "File Downloaded", basename(str_sub(eventLabel, start = -259)), eventLabel))
#
# other_events_data <- other_events
#
#
# data_downloads <- as.data.table(other_events_data %>%
#                                   filter(eventCategory == "Downloads")) %>%
#   filter(pagePath == "/download-latest-data" | grepl("Publication: ", eventItem))
#
# downloads_lookup <- release_events_data %>%
#   filter(eventType == "File Downloaded") %>%
#   dplyr::select(publication, eventItem) %>%
#   distinct()
#
#
# release_distinct_lookup <- as.data.table(release_pages_data %>%
#                                            select(publication, release, last_edited) %>%
#                                            distinct()) %>%
#   # filter out MAT and pupil absence old releases published on the same day as the current release
#   subset(!(publication == "Multi-academy trust performance measures at key stage 2" & release == "Academic Year 2017/18")) %>%
#   subset(!(publication == "Pupil absence in schools in England" & release == "Academic Year 2016/17"))
#
# data_downloads_joined <- data_downloads %>%
#   base::merge(as.data.table(downloads_lookup), by.x = "eventItem", by.y = "eventItem", all.x = TRUE) %>%
#   dplyr::select(
#     date,
#     publication,
#     eventAction,
#     eventLabel,
#     totalEvents
#   ) %>%
#   left_join(release_distinct_lookup, by = "publication") %>%
#   filter(date >= last_edited | is.na(last_edited)) %>%
#   group_by(date, publication, eventAction, eventLabel, totalEvents) %>%
#   filter(last_edited == max(last_edited) | is.na(last_edited))
#
#
# data_downloads_joined_complete <- data_downloads_joined %>%
#   filter(!is.na(publication))
#
# # fill in the missing publications
# missing_pubs <- data_downloads_joined %>%
#   filter(is.na(publication) & !grepl("Data Catalogue Page Selected", eventAction) & !grepl("Release Page All Files Downloaded", eventAction)) %>%
#   mutate(
#     publication_link = str_replace(eventLabel, "File URL: /Api/download/", ""),
#     publication_link = str_replace(publication_link, "/.*", ""),
#     publication_link = str_replace(publication_link, "Publication:", ""),
#     publication_link = str_replace(publication_link, ", File:.*", ""),
#     publication_link = to_snake_case(publication_link)
#   ) %>%
#   base::merge(lookup_publication, by.x = "publication_link", by.y = "publication_link", all.x = TRUE) %>%
#   dplyr::select(date,
#                 publication = publication.y,
#                 eventAction,
#                 eventLabel,
#                 totalEvents
#   ) %>%
#   left_join(release_distinct_lookup, by = "publication") %>%
#   filter(date >= last_edited) %>%
#   group_by(date, publication, eventAction, eventLabel, totalEvents) %>%
#   filter(last_edited == max(last_edited))
#
# # Change of format with data catalogue
# missing_pubs2 <- data_downloads_joined %>%
#   filter(is.na(publication) &
#            (grepl("Data Catalogue Page Selected", eventAction) | grepl("Release Page All Files Downloaded", eventAction))) %>%
#   mutate(file_count = str_count(eventLabel, ".csv")) %>%
#   filter(file_count <= 1) %>%
#   mutate(
#     publication_link = str_replace(eventLabel, "File URL: /Api/download/", ""),
#     publication_link = str_replace(publication_link, "/.*", ""),
#     publication_link = str_replace(publication_link, "Publication:", ""),
#     publication_link = str_replace(publication_link, ", Release:.*", ""),
#     # publication_link = str_replace(publication_link, ", File:.*",""),
#     publication_link = to_snake_case(publication_link)
#   ) %>%
#   mutate(
#     release_file = str_replace(eventLabel, "File URL: /Api/download/", ""),
#     release_file = str_replace(release_file, "/.*", ""),
#     release_file = str_replace(release_file, "Publication: ", ""),
#     release_file = str_replace(release_file, ".*Release: ", ""),
#     release_file = str_replace(release_file, "File:.*", "")
#   ) %>%
#   mutate(release_file = if_else(grepl("Download Latest Data Page", eventAction),
#                                 NA_character_, as.character(release_file)
#   )) %>%
#   base::merge(lookup_publication, by.x = "publication_link", by.y = "publication_link", all.x = TRUE) %>%
#   dplyr::select(date,
#                 publication = publication.y,
#                 release_file,
#                 eventAction,
#                 eventLabel,
#                 totalEvents
#   ) %>%
#   left_join(release_distinct_lookup, by = "publication") %>%
#   filter(date >= last_edited) %>%
#   group_by(date, publication, release_file, eventAction, eventLabel, totalEvents) %>%
#   filter(last_edited == max(last_edited)) %>%
#   mutate(release = if_else(is.na(release_file), as.character(release), as.character(release_file))) %>%
#   ungroup() %>%
#   select(-release_file)
#
# # Change of format with data catalogue - multiple file downloads
# missing_pubs_multi <- data_downloads_joined %>%
#   filter(is.na(publication) &
#            (grepl("Data Catalogue Page Selected", eventAction) | grepl("Release Page All Files Downloaded", eventAction))) %>%
#   mutate(file_count = str_count(eventLabel, ".csv")) %>%
#   filter(file_count > 1) %>%
#   mutate(
#     publication_link = str_replace(eventLabel, "File URL: /Api/download/", ""),
#     publication_link = str_replace(publication_link, "/.*", ""),
#     publication_link = str_replace(publication_link, "Publication:", ""),
#     publication_link = str_replace(publication_link, ", Release:.*", ""),
#     # publication_link = str_replace(publication_link, ", File:.*",""),
#     publication_link = to_snake_case(publication_link)
#   ) %>%
#   mutate(
#     release_file = str_replace(eventLabel, "File URL: /Api/download/", ""),
#     release_file = str_replace(release_file, "/.*", ""),
#     release_file = str_replace(release_file, "Publication: ", ""),
#     release_file = str_replace(release_file, ".*Release: ", ""),
#     release_file = str_replace(release_file, "File:.*", "")
#   ) %>%
#   mutate(release_file = if_else(grepl("Download Latest Data Page", eventAction),
#                                 NA_character_, as.character(release_file)
#   )) %>%
#   base::merge(lookup_publication, by.x = "publication_link", by.y = "publication_link", all.x = TRUE) %>%
#   dplyr::select(date,
#                 publication = publication.y,
#                 release_file,
#                 eventAction,
#                 eventLabel,
#                 totalEvents
#   ) %>%
#   left_join(release_distinct_lookup, by = "publication") %>%
#   filter(date >= last_edited) %>%
#   group_by(date, publication, release_file, eventAction, eventLabel, totalEvents) %>%
#   filter(last_edited == max(last_edited)) %>%
#   mutate(release = if_else(is.na(release_file), as.character(release), as.character(release_file))) %>%
#   ungroup() %>%
#   select(-release_file) %>%
#   mutate(
#     eventLabel = str_replace(eventLabel, ".*File: ", ""),
#     eventLabel = strsplit(as.character(eventLabel), ",")
#   ) %>%
#   unnest(eventLabel) %>%
#   mutate(eventLabel = str_replace(eventLabel, " ", ""))
#
# # Change of format with data catalogue - all file downloads
# all_file_downloads <- as.data.table(release_events_data) %>%
#   filter(grepl("Release Page All Files downloads", eventAction)) %>%
#   mutate(eventLabel == "All Files") %>%
#   dplyr::select(
#     date,
#     publication,
#     release,
#     last_edited,
#     eventAction,
#     eventLabel,
#     totalEvents
#   )
#
# data_downloads_final <- as.data.table(release_events_data %>%
#                                         filter(eventType == "File Downloaded") %>%
#                                         dplyr::select(
#                                           date,
#                                           publication,
#                                           release,
#                                           last_edited,
#                                           eventAction,
#                                           eventLabel,
#                                           totalEvents
#                                         )) %>%
#   rbind(data_downloads_joined_complete) %>%
#   rbind(missing_pubs) %>%
#   rbind(missing_pubs2) %>%
#   rbind(missing_pubs_multi) %>%
#   rbind(all_file_downloads) %>%
#   filter(!grepl("ancillary", eventLabel)) %>%
#   mutate(
#     eventAction = str_replace(eventAction, "Release Page ", ""),
#     eventAction = str_replace(eventAction, "Download Latest Data Page ", ""),
#     eventLabel = str_replace(eventLabel, ".*data/", ""),
#     eventLabel = str_replace(eventLabel, ".*File: ", ""),
#     eventLabel = if_else(grepl(".zip", eventLabel), "All Files", as.character(eventLabel))
#   )
#
#
#
# data_downloads_chart <- filter_on_date(data_downloads_final) %>%
#   ungroup() %>%
#   group_by(date) %>%
#   summarise(totalEvents = sum(totalEvents)) %>%
#   mutate(highlight_flag = ifelse(format(as.Date(date), "%a") %in% c("Sat", "Sun"), T, F))
#
#
# data_downloads_chart_ind <- filter_on_date(data_downloads_final) %>%
#   ungroup() %>%
#   filter(grepl(".csv", eventLabel)) %>%
#   group_by(date) %>%
#   summarise(totalEvents = sum(totalEvents)) %>%
#   mutate(highlight_flag = ifelse(format(as.Date(date), "%a") %in% c("Sat", "Sun"), T, F))
#
#
#
# data_downloads_chart_zip <- filter_on_date(data_downloads_final) %>%
#   ungroup() %>%
#   filter(eventLabel == "All Files") %>%
#   group_by(date) %>%
#   summarise(totalEvents = sum(totalEvents)) %>%
#   mutate(highlight_flag = ifelse(format(as.Date(date), "%a") %in% c("Sat", "Sun"), T, F))
#
# data_downloads_chart_ancil <- filter_on_date(data_downloads_final) %>%
#   ungroup() %>%
#   subset(!grepl(".csv", eventLabel) & eventLabel != "All Files") %>%
#   group_by(date) %>%
#   summarise(totalEvents = sum(totalEvents)) %>%
#   mutate(highlight_flag = ifelse(format(as.Date(date), "%a") %in% c("Sat", "Sun"), T, F))








# Table creation stuff -------

UA_table_events <- tbl(connection, "GAapi_events") %>%
  filter(eventCategory == "Table Tool" | eventCategory == "Publication Release Data Tabs") %>%
  as.data.frame()

UA_table_events <- UA_table_events %>%
  mutate(data = "Universal analytics") %>%
  mutate(url = paste0("https://explore-education-statistics.service.gov.uk", pagePath))

# rename to match GA4 colnames
UA_table_events <- UA_table_events %>% rename(eventCount = totalEvents)
UA_table_events <- UA_table_events %>% rename(eventName = eventAction)

GA4_table_events <- ga_data(
  369420610,
  metrics = c("eventCount"),
  dimensions = c("date", "pagePath", "eventName", "customEvent:event_label", "customEvent:event_category"),
  dim_filters = ga_data_filter("customEvent:event_category" == "Table Tool" | "customEvent:event_category" == "Publication Release Data Tabs"),
  date_range = c("2023-06-02", "2024-09-09"),
  limit = -1
)

GA4_table_events <- GA4_table_events %>%
  mutate(data = "GA4") %>%
  mutate(url = paste0("https://explore-education-statistics.service.gov.uk", pagePath))

# tidy col names
GA4_table_events <- GA4_table_events %>% rename(eventLabel = "customEvent:event_label")
GA4_table_events <- GA4_table_events %>% rename(eventCategory = "customEvent:event_category")

# # #Combine data

names(UA_table_events)
names(GA4_table_events)

combined_table_events <-
  rbind(
    select(
      UA_table_events,
      c("data", "date", "pagePath", "url", "eventName", "eventLabel", "eventCategory", "eventCount")
    ),
    select(
      GA4_table_events,
      c("data", "date", "pagePath", "url", "eventName", "eventLabel", "eventCategory", "eventCount")
    )
  )

# # # checks
nrow(combined_table_events) # 756996
sum(combined_table_events$eventCount) # 1712677

nrow(UA_table_events) + nrow(GA4_table_events) # 756996
sum(UA_table_events$eventCount) + sum(GA4_table_events$eventCount) # 1712677

# # #join publication info
combined_table_events_joined <- left_join(
  combined_table_events,
  scrape_data,
  by = c("url" = "url") # ,
)

# # checks
nrow(combined_table_events_joined) # 756996
sum(combined_table_events_joined$eventCount) # 1712677

# # write file
write_csv(combined_table_events_joined, "data/GA4_table_events.csv")




# old table creation cleaning code ----
# This is very messy and needs refactoring

# # Fix table tool data
# table_tool_data <- other_events_data %>%
#   filter(eventAction == "Publication and Subject Chosen") %>%
#   # filter(!str_detect(paste0("https://explore-education-statistics.service.gov.uk",pagePath),"https://explore-education-statistics.service.gov.uk/data-tables/fast-track/")) %>%
#   separate(col = eventLabel, into = c("publication_event", "subject"), sep = "\\/", extra = "merge") %>% # separate out the release and subject name
#   mutate(
#     publication_event = str_replace(publication_event, "Key Stage 4 Performance \\(revised\\)", "Key stage 4 performance"),
#     publication_event = str_replace(publication_event, "Secondary and Primary School applications", "Secondary and Primary School applications and offers"),
#     publication_event = str_replace(publication_event, "NEET Statistics Annual brief", "NEET annual brief"),
#     publication_link = to_snake_case(publication_event)
#   ) %>%
#   left_join(lookup_publication, by = "publication_link") %>%
#   unique()






# Search terms ----

UA_search_events <- tbl(connection, "GAapi_events") %>%
  filter(eventAction == "PageSearchForm") %>%
  as.data.frame()

UA_search_events <- UA_search_events %>%
  mutate(data = "Universal analytics") %>%
  mutate(url = paste0("https://explore-education-statistics.service.gov.uk", pagePath))

# rename to match GA4 colnames
UA_search_events <- UA_search_events %>% rename(eventCount = totalEvents)
UA_search_events <- UA_search_events %>% rename(eventName = eventAction)

GA4_search_events <- ga_data(
  369420610,
  metrics = c("eventCount"),
  dimensions = c("date", "pagePath", "eventName", "customEvent:event_label", "customEvent:event_category"),
  dim_filters = ga_data_filter("eventName" == "PageSearchForm"),
  date_range = c("2023-06-02", "2024-09-09"),
  limit = -1
)

GA4_search_events <- GA4_search_events %>%
  mutate(data = "GA4") %>%
  mutate(url = paste0("https://explore-education-statistics.service.gov.uk", pagePath))

# tidy col names
GA4_search_events <- GA4_search_events %>% rename(eventLabel = "customEvent:event_label")
GA4_search_events <- GA4_search_events %>% rename(eventCategory = "customEvent:event_category")

# # #Combine data

names(UA_search_events)
names(GA4_search_events)

combined_search_events <-
  rbind(
    select(
      UA_search_events,
      c("data", "date", "pagePath", "url", "eventName", "eventLabel", "eventCategory", "eventCount")
    ),
    select(
      GA4_search_events,
      c("data", "date", "pagePath", "url", "eventName", "eventLabel", "eventCategory", "eventCount")
    )
  )

# # checks
nrow(combined_search_events) # 163102
sum(combined_search_events$eventCount) # 208779

nrow(UA_search_events) + nrow(GA4_search_events) # 163102
sum(UA_search_events$eventCount) + sum(GA4_search_events$eventCount) # 208779

# # #join publication info
combined_search_events_joined <- left_join(
  combined_search_events,
  scrape_data,
  by = c("url" = "url") # ,
)

# # checks
nrow(combined_search_events_joined) # 163102
sum(combined_search_events_joined$eventCount) # 208779

# # write file
write_csv(combined_search_events_joined, "data/GA4_search_events.csv")
