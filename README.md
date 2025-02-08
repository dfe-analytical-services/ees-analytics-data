[![lintr](https://github.com/dfe-analytical-services/explore-education-statistics-analytics/actions/workflows/lintr.yml/badge.svg)](https://github.com/dfe-analytical-services/explore-education-statistics-analytics/actions/workflows/lintr.yml)

# Explore education statistics analytics 

Analysis of analytics data for our explore education statistics (EES) service, pulling in data from a number of sources to disseminate out. Primarily shared with publishers on the service to understand usage of their content, and with the EES service team for assessing service performance.

There are two main parts to this repository:
1. Ad hoc analysis scripts (`adhoc-scripts/`)
2. Data processing and update pipelines (`data-updates/`)

Guides for each of these are provided in the README files in the respective folders. There is a [high level diagram of the analytics data ingestion and processing](https://lucid.app/lucidchart/97ee2663-4065-425e-92df-dd664d44973d/edit?viewport_loc=-835%2C-412%2C2632%2C1302%2C0_0&invitationId=inv_1289a047-b729-46bc-85ef-425229b540a5).

## Analytics dashboard

There is an R Shiny dashboard that sits on top of this data in a separate GitHub repository - https://github.com/dfe-analytical-services/explore-education-statistics-analytics-dashboard.

## Contributing

If you want to make edits to the code or run anything locally, start by familiarising yourself with the specific README in the part of the project you're intending to use.

### Code styling 

We have some pre-commit hooks set up to help with code quality. These are controlled by the `.hooks/pre-commit.R` file. Two functions you should run regularly if contributing are:

```r
styler::style_dir()
lintr::lint_dir()
```

This will style code according to tidyverse styling. lintr will check all pull requests and fail if code is not appropriately formatted.

## Contact

explore.statistics@education.gov.uk