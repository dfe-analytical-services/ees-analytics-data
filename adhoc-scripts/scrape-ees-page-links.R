scrape_ees_page_links <- function() {
  message("------------------------------------------------------------------")
  message("Scraping")
  message("------------------------------------------------------------------")
  message("")

  message("Starting scraping...")

  start <- as.numeric(Sys.time())

  # Scraping functions --------------------------------------------------------
  scrape_node_href <- function(url, node_type) {
    read_html(url) %>%
      html_nodes(node_type) %>%
      html_attr("href")
  }

  message("...scraping for service pages...")

  # Get main service page links -----------------------------------------------
  homepage_domain <- "https://explore-education-statistics.service.gov.uk"

  service_pages <- c(
    scrape_node_href(homepage_domain, ".govuk-link"), # link text
    scrape_node_href(homepage_domain, ".govuk-button") # button links
  )

  message("...scraping find stats pages for publications...")

  # Get publications ----------------------------------------------------------
  find_stats_url <- paste0(homepage_domain, "/find-statistics")

  # !!!! This is broken, hard coding to 10
  # # Get the number of pages
  # # not all pages are shown straight away so need to work out what exists
  # number_of_pages <- scrape_node_href(find_stats_url, ".govuk-link") %>%
  #   str_subset("page") %>% # includes page 2 twice due to 'next' button
  #   str_sub(start = -1) %>% # take out final character (page numbers)
  #   max() # work out number of pages present

  # Create URLs for each find stats page
  find_stats_pages <- sapply(
    1:10,
    function(x) paste0("/find-statistics?page=", x)
  )

  # Extract publication list
  # slug is the publication name part of the url
  scraped_publications <- lapply(
    find_stats_pages,
    function(x) {
      scrape_node_href(paste0(homepage_domain, x), ".govuk-link") %>%
        # using this to filter to only publication links
        str_subset("/find-statistics/")
    }
  ) %>%
    unlist()

  message("...checking number of publications scraped...")

  # QA against expected publications ------------------------------------------
  # Scrape the number of 'results' showing on find stats page
  expected_number <- read_html(find_stats_url) %>%
    html_elements("h2") %>%
    html_text2() %>%
    str_subset("results") %>%
    str_remove(" results")

  # Compare with number of extracted publications
  difference <- as.numeric(expected_number) - length(scraped_publications)

  if (difference != 0) {
    stop(
      paste0(
        "The scraping of publications from the find statistics page is broken",
        ", please investigate before proceeding. Lines 21-52 in scrape_ees.R"
      )
    )
  } else {
    message(
      "...number of publications scraped matches expected number,",
      " continuing scrape..."
    )
  }

  message("...scraping for methodology pages...")

  # Get methodologies ---------------------------------------------------------
  methodology_url <-
    "https://explore-education-statistics.service.gov.uk/methodology"

  scraped_methodologies <- scrape_node_href(methodology_url, ".govuk-link") %>%
    str_subset("/methodology/") # filter to only methodology links

  message("...scraping for remaining pages, may take a few minutes...")
  # Scrape all potential pages ------------------------------------------------
  # e.g. past releases, data guidance pages
  list_all_potential_pages <-
    lapply(
      paste0(
        homepage_domain,
        c(service_pages, scraped_publications, scraped_methodologies)
      ),
      function(x) {
        paste(x)
        # to add links in scrape_node_href(x, ".govuk-link")
      }
    ) %>%
    unlist() %>%
    unique()

  # there's a dodgy few...
  list_all_potential_pages <- rlist::list.filter(list_all_potential_pages, . != "https://explore-education-statistics.service.gov.ukhttps://www.gov.uk/search/research-and-statistics?content_store_document_type=upcoming_statistics&organisations%5B%5D=department-for-education&order=updated-newest") # noline: object_usage_linter

  list_all_potential_pages <- rlist::list.filter(list_all_potential_pages, . != "https://explore-education-statistics.service.gov.ukhttps://osr.statisticsauthority.gov.uk/what-we-do/")

  list_all_potential_pages <- rlist::list.filter(list_all_potential_pages, . != "https://explore-education-statistics.service.gov.ukhttps://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/")

  list_all_potential_pages <- rlist::list.filter(list_all_potential_pages, . != "https://explore-education-statistics.service.gov.ukhttps://www.nationalarchives.gov.uk/information-management/re-using-public-sector-information/uk-government-licensing-framework/crown-copyright/")

  all_potential_pages <-
    lapply(list_all_potential_pages, function(x) {
      scrape_node_href(x, ".govuk-link")
    }) %>%
    unlist() %>%
    unique() %>%
    as.data.frame()

  # Filter to pages we're interested in
  cleaned_potential_pages <-
    all_potential_pages %>%
    filter(startsWith(., "/find-statistics/") | startsWith(., "/methodology/") | startsWith(., "/data-tables/")) %>%
    as.vector() %>%
    unlist(use.names = FALSE)

  scraped_pages <- c(
    cleaned_potential_pages,
    service_pages,
    scraped_publications,
    scraped_methodologies
  ) %>%
    unique()

  message("...scraping information from all scraped pages, will definitely take a few minutes...")

  # OLD CODE ---------------------------------------------------------------------------
  # decided just to edit the variables in this and leave tidying for a later point as it seems to still work now I've fixed the first bit

  # Function to take key information from service pages
  get_page_info <- function(a) {
    url <- tryCatch(
      read_html(
        paste0(
          "https://explore-education-statistics.service.gov.uk",
          a
        )
      ),
      error = function(e) {
        read_html("https://explore-education-statistics.service.gov.uk")
      }
    )

    output <- cbind(
      # return path
      paste0(
        "https://explore-education-statistics.service.gov.uk",
        a
      ),
      # page heading
      url %>%
        html_elements("h1.govuk-heading-xl") %>%
        html_text2() %>%
        {
          if (length(.) == 0) {
            NA
          } else {
            .
          }
        },
      # release info
      url %>%
        html_elements("span.govuk-caption-xl") %>%
        html_text2() %>%
        {
          if (length(.) == 0) {
            NA
          } else {
            .
          }
        },
      # last updated
      url %>%
        html_elements("time") %>%
        html_text2() %>%
        {
          if (length(.) == 0) {
            NA
          } else {
            .[1]
          }
        }, # Need the [1] or else next published date will be pulled through as an additional row in the list

      # Publication (table tool pages only)
      url %>%
        html_elements("dd.govuk-summary-list__value") %>%
        html_text2() %>%
        {
          if (startsWith(a, "/data-tables/")) {
            .[1]
          } else {
            NA
          }
        }
    )


    return(output)
  }

  # Apply the function across all the pages we're interested in
  expected_pages_with_info <-
    lapply(scraped_pages, get_page_info)

  # Turn results into a dataframe
  scraped_info <-
    as.data.frame(do.call(rbind, lapply(
      expected_pages_with_info, as.data.frame
    )))

  colnames(scraped_info) <-
    c("url", "heading", "release", "last_edited", "pub")

  # Remove any duplicates
  scraped_info <- unique(scraped_info)

  # Adding publication and time period columns
  scraped_info <- scraped_info %>%
    mutate(
      publication = ifelse((grepl("find-statistics/", url) | grepl("methodology/", url)), heading, ifelse(grepl("data-tables/", url), pub, NA)),
      time_period = ifelse(
        grepl("Week", release),
        paste(release),
        ifelse(
          grepl(
            "January|February|March|April|May|June|July|August|September|October|November|December",
            release
          ),
          paste(release),
          gsub("[^[:digit:]/-]", "", release)
        )
      )
    )

  # Force in a latest release column so it's easy to extra these rows later (I don't know why either, other than the code relies on it)

  scraped_info <- scraped_info %>% mutate(is_latest_release = "No")

  # You then need to force in the release specific links for the latest release for each publication as these don't exist anywhere within the service

  # Creating the latest release links
  latest_release_pages <- scraped_info %>%
    filter(grepl("find-statistics/", url)) %>%
    filter(str_count(url, "/") == 4) %>%
    mutate(is_latest_release = "Yes") %>% # changing flag to be latest release for this set of rows
    mutate(url = tolower(ifelse(
      grepl("Week", release),
      paste0(
        url,
        "/",
        word(time_period, 3),
        "-",
        word(time_period, 1),
        "-",
        word(time_period, 2)
      ),
      ifelse(
        grepl(
          c(
            "January|February|March|April|May|June|July|August|September|October|November|December"
          ),
          release
        ),
        paste0(url, "/", word(time_period, 2), "-", word(time_period, 1)),
        paste0(url, "/", mgsub::mgsub(time_period, c("/", " "), c("-", "-")))
      )
    )))

  output <- rbind(scraped_info, latest_release_pages)

  message("Writing scrape data to database...")

  dbWriteTable(
    conn = connection,
    name = Id(
      schema = "dbo",
      table = "ees_scrape_expected_service_pages"
    ),
    value = output,
    overwrite = TRUE
  )

  end <- as.numeric(Sys.time())

  pretty_time <- present_time_neatly(start, end)

  message("")
  message("-------------------------------------------------------------------")
  message("EES pages scraped in ", pretty_time)
  message("-------------------------------------------------------------------")
  message("")

  # Adding to data frame and outputting to give final scrape info
  # return(output)
}
