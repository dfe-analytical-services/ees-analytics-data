[![lintr](https://github.com/dfe-analytical-services/explore-education-statistics-analytics/actions/workflows/lintr.yml/badge.svg)](https://github.com/dfe-analytical-services/explore-education-statistics-analytics/actions/workflows/lintr.yml)

# Explore education statistics analytics 

Analysis of analytics data for our explore education statistics (EES) service, pulling in data from a number of sources to disseminate out. Primarily shared with publishers on the service to understand usage of their content, and with the EES service team for assessing service performance.

There are two main parts to this repository:
1. Ad hoc analysis scripts (`adhoc-scripts/`)
2. Data processing and update pipelines (`data-updates/`)

Guides for each of these are provided in the README files in the respective folders. There is a [high level diagram of the analytics data ingestion and processing](https://lucid.app/lucidchart/97ee2663-4065-425e-92df-dd664d44973d/edit?viewport_loc=-835%2C-412%2C2632%2C1302%2C0_0&invitationId=inv_1289a047-b729-46bc-85ef-425229b540a5).

## Ad hoc scripts

Assorted collection of scripts used in ad hoc analysis that may or may not be useful and may or may not work.

## Data processing and update pipelines

Code used to extract source data, process it, and save a permanent store for usage by the analytics dashboard.

## Analytics dashboard

There is an R Shiny dashboard that sits on top of this data in a separate GitHub repository - https://github.com/dfe-analytical-services/explore-education-statistics-analytics-dashboard.

## Access requirements

To run any of the code in this repo, you may need to contact the maintainers to gain access to the source data:
- Google Analytics (GA) property 
- Google Search Console property (separate to GA!)
- Access to the database where we have a permanent store of analytics data
  
## Contributing

If you want to make edits to the code or run anything locally, start by familiarising yourself with the specific README in the part of the project you're intending to use.

### Flagging issues

If you spot any issues with the application, please flag it in the "Issues" tab of this repository, and label as a bug. Include as much detail as possible to help us diagnose the issue and prepare a suitable remedy.

### Making suggestions

You can also use the "Issues" tab in GitHub to suggest new features, changes or additions. Include as much detail on why you're making the suggestion and any thinking towards a solution that you have already done.

### Navigation

In general all `.R` files will have a usable outline, so make use of that for navigation if in RStudio: `Ctrl-Shift-O`.

### Code styling 

The function `styler::style_dir()` will tidy code according to tidyverse styling using the styler package. Run this regularly as our pre-commit hooks will prevent you committing code that isn't tidied. This function also helps to test the running of the code and for basic syntax errors such as missing commas and brackets.

You should also run `lintr::lint_dir()` regularly as lintr will check all pull requests for the styling of the code, it does not style the code for you like styler, but is slightly stricter and checks for long lines, variables not using snake case, commented out code and undefined objects amongst other things.

### Pre-commit hooks

We have some pre-commit hooks set up to help with code quality. These are controlled by the `.hooks/pre-commit.R` file.

## Contact

explore.statistics@education.gov.uk