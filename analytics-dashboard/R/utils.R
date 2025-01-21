filter_on_date <- function(data, period) {
  first_date <- if (period == "week") {
    week_date
  } else if (period == "four_week") {
    four_week_date
  } else if (period == "since_2ndsep") {
    since_4thsep_date
  } else if (period == "six_month") {
    six_month_date
  } else if (period == "one_year") {
    one_year_date
  } else if (period == "all_time") {
    all_time_date
  } else {
    "2020-04-03"
  }

  data %>% filter(date >= first_date & date <= latest_date)
}

filter_on_date_pub <- function(data, period, page) {
  first_date <- if (period == "week") {
    week_date
  } else if (period == "four_week") {
    four_week_date
  } else if (period == "since_4thsep") {
    since_4thsep_date
  } else if (period == "six_month") {
    six_month_date
  } else if (period == "one_year") {
    one_year_date
  } else if (period == "all_time") {
    all_time_date
  } else {
    "2020-04-03"
  }

  data %>%
    filter(date >= first_date & date <= latest_date) %>%
    filter(publication == page)
}
