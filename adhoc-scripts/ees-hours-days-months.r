# Databricks notebook source
# MAGIC %md
# MAGIC A few quick summaries of the usage on EES, breaking down by the most popular hours, days and months

# COMMAND ----------

# DBTITLE 1,Set up dependencies
options(repos = c(CRAN = "https://packagemanager.posit.co/cran/__linux__/focal/latest"))

if (!requireNamespace("pak", quietly = TRUE)) {
  install.packages("pak")
}

pkgs <- c("sparklyr", "arrow", "dplyr", "ggplot2", "afcharts", "stringr")

to_install <- pkgs[!sapply(pkgs, requireNamespace, quietly = TRUE)]

if (length(to_install) > 0) {
  pak::pkg_install(to_install, ask = FALSE)
} else {
  message("Skipping install... all packages already installed")
}

lapply(pkgs, library, character.only = TRUE)

catalog <- "catalog_40_copper_statistics_services."
schema <- "analytics_raw."

sc <- spark_connect(method = "databricks")

# COMMAND ----------

# DBTITLE 1,By month
monthly_breakdown <- sparklyr::sdf_sql(sc, paste0("
  SELECT 
  DATE_FORMAT(date, 'MMMM') AS month,
  SUM(sessions) AS sessions
FROM 
  (SELECT date, sessions FROM ", catalog, schema, "ees_ga4_service_summary
   UNION ALL
   SELECT date, sessions FROM ", catalog, schema, "ees_ua_service_summary) AS combined_summary
WHERE
  YEAR(date) in (2021,2022,2023,2024)
GROUP BY 
  DATE_FORMAT(date, 'MMMM'),
  MONTH(date)
ORDER BY 
  MONTH(date);
")) |> 
  collect()

ggplot(monthly_breakdown, aes(x = sessions, y = factor(month, levels = rev(month.name)))) +
  geom_col(fill = af_colour_values["dark-blue"]) +
  theme_af(grid = "none", axis = "none") +
  geom_text(aes(label = scales::comma(sessions)), color = "white", position = position_stack(vjust = 0), hjust = -0.15) +
  labs(
    x = NULL,
    y = NULL,
    title = "November is our busiest month",
    subtitle = stringr::str_wrap(
      "Total number of sessions on explore education statistics by month, 2021-2024 inclusive",
      60
    ),
    caption = "Source: Google Universal Analytics and Google Analytics 4"
  ) +
  theme(plot.caption = element_text(margin = margin(t = 20))) +
  theme(plot.margin = unit(c(1, 5, 1, 0.5), "cm")) +
  theme(axis.text.x = element_blank()) +
  theme(axis.line.y = element_blank())

# COMMAND ----------

# DBTITLE 1,By day
daily_breakdown <- sparklyr::sdf_sql(sc, paste0("
  SELECT
  DATE_FORMAT(date, 'EEEE') AS day_of_week,
  AVG(sessions) AS sessions
FROM
  (
    SELECT
      date,
      sessions
    FROM
      ", catalog, schema, "ees_ga4_service_summary
    UNION ALL
    SELECT
      date,
      sessions
    FROM
      ", catalog, schema, "ees_ua_service_summary
  ) AS combined_summary
GROUP BY
  DATE_FORMAT(date, 'EEEE')
ORDER BY
  CASE DATE_FORMAT(date, 'EEEE')
    WHEN 'Sunday' THEN 1
    WHEN 'Monday' THEN 2
    WHEN 'Tuesday' THEN 3
    WHEN 'Wednesday' THEN 4
    WHEN 'Thursday' THEN 5
    WHEN 'Friday' THEN 6
    WHEN 'Saturday' THEN 7
  END;
")) |> 
  collect()

ggplot(daily_breakdown, aes(x = sessions, y = factor(day_of_week, levels = rev(c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"))))) +
  geom_col(fill = af_colour_values["dark-blue"]) +
  theme_af(grid = "none", axis = "none") +
  geom_text(aes(label = scales::comma(sessions)), color = "white", position = position_stack(vjust = 0), hjust = -0.15) +
  labs(
    x = NULL,
    y = NULL,
    title = "Tuesday pips Thursday as our busiest day",
    subtitle = stringr::str_wrap(
      "Average sessions on explore education statistics by day, all time",
      60
    ),
    caption = "Source: Google Universal Analytics and Google Analytics 4"
  ) +
  theme(plot.caption = element_text(margin = margin(t = 20))) +
  theme(plot.margin = unit(c(1, 5, 1, 0.5), "cm")) +
  theme(axis.text.x = element_blank()) +
  theme(axis.line.y = element_blank())

# COMMAND ----------

# DBTITLE 1,By hour
hourly_breakdown <- sparklyr::sdf_sql(sc, paste0("
  SELECT 
    cast(hour as int), 
    sum(sessions) as sessions 
  FROM ", catalog, schema, "ees_ga4_hourly 
  GROUP BY hour 
  ORDER BY hour;
")) |> 
  collect() |>
  filter(!is.na(hour))

ggplot(hourly_breakdown, aes(x = sessions, y = factor(hour, levels = rev(0:23), labels = sprintf("%02d:00", rev(0:23))))) +
  geom_col(fill = af_colour_values["dark-blue"]) +
  theme_af(grid = "none", axis = "none") +
  geom_text(aes(label = scales::comma(sessions)), color = "black", position = position_stack(vjust = 1), hjust = -0.1) +
  labs(
    x = NULL,
    y = NULL,
    title = "Interest peaks at 10am and 2pm",
    subtitle = stringr::str_wrap(
      "Active sessions by hour on explore education statistics, since 2022",
      60
    ),
    caption = "Source: Google Analytics 4"
  ) +
  theme(plot.caption = element_text(margin = margin(t = 20))) +
  theme(plot.margin = unit(c(1, 5, 1, 0.5), "cm")) +
  theme(axis.text.x = element_blank()) +
  theme(axis.line.y = element_blank()) +
  coord_cartesian(xlim = c(0, max(hourly_breakdown$sessions) * 1.2))
