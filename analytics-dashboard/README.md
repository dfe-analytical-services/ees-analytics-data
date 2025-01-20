# Explore education statistics analytics - dashboard

This is an R Shiny dashboard that is deployed via the DfE POSIT Connect subscription internally. There are three environments, all accessible to DfE AD:

* Production - https://rsconnect/rsc/ees-analytics/
* Pre-production - https://rsconnect-pp/rsc/ees-analytics/
* Development - https://rsconnect-pp/rsc/dev-ees-analytics/

### Data updates

Source data for this dashboard is created and managed in the data-updates folder.

## Requirements

### i. Software requirements (for running locally)

- Installation of R Studio 2024.04.2+764 "Chocolate Cosmos" or higher

- Installation of R 4.4.1 or higher

- Installation of RTools44 or higher

### ii. Programming skills required (for editing or troubleshooting)

- R at an intermediate level, [DfE R learning resources](https://dfe-analytical-services.github.io/analysts-guide/learning-development/r.html)

- Particularly [R Shiny](https://shiny.rstudio.com/)

### iii. Access requirements

To run the dashboard locally:
- Access to the databases where we have a permanent store of analytics data

## Contributing to the dashboard

#### Running locally

Package control is handled using [renv](https://rstudio.github.io/renv/articles/renv.html) at the top level of the repository.

1. Clone or download the repo. 

2. Open the R project in R Studio.

3. Run `renv::restore()` to install dependencies.

4. Run `shiny::runApp("analytics-dashboard")` to run the dashboard locally.

#### Tests

Automated tests have been created using shinytest2 that test the app loads and also give other examples of ways you can use tests. You should edit the tests as you add new features into the app and continue to add and maintain the tests over time.

GitHub Actions provide CI by running the automated tests and checks for code styling on every pull request into the main branch. The yaml files for these workflows can be found in the .github/workflows folder.

You should run `shinytest2::test_app("analytics-dashboard")` regularly to check that the tests are passing against the code you are working on.
