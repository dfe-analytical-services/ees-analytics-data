# Explore education statistics analytics 

---

## Introduction 

Analysis of analytics data for our explore education statistics service.

There are three main parts to this repository:
1. Ad hoc analysis scripts (`adhoc-scripts/` folder)
2. Data processing and update pipelines (`data-updates/` folder)
3. Analytics dashboard (`analytics-dashboard/` folder)

### Analytics dashboard

There is an R Shiny dashboard that is deployed via the DfE POSIT Connect subscription internally. There are three environments, all accessible to DfE AD:

* Production - https://rsconnect/rsc/ees-analytics/
* Pre-production - https://rsconnect-pp/rsc/ees-analytics/
* Development - https://rsconnect-pp/rsc/dev-ees-analytics/

### Data updates

TBC...

---

## Requirements

### i. Software requirements (for running locally)

- Installation of R Studio 2024.04.2+764 "Chocolate Cosmos" or higher

- Installation of R 4.4.1 or higher

- Installation of RTools44 or higher

### ii. Programming skills required (for editing or troubleshooting)

- R at an intermediate level, [DfE R learning resources](https://dfe-analytical-services.github.io/analysts-guide/learning-development/r.html)

- Particularly [R Shiny](https://shiny.rstudio.com/)

### iii. Access requirements

To connect to the source data for any ad hoc analysis you may need:
- Permissions set on the Google Analytics (GA) property 
- Permissions set on the Google Search Console property (separate to GA!)
- Access to the database where we have a permanent store of analytics data

To run the dashboard locally:
- Access to the databases where we have a permanent store of analytics data

To run the data update pipelines:
- Access to all of the above
  
---

## Contributing

### Flagging issues

If you spot any issues with the application, please flag it in the "Issues" tab of this repository, and label as a bug. Include as much detail as possible to help us diagnose the issue and prepare a suitable remedy.

### Making suggestions

You can also use the "Issues" tab in GitHub to suggest new features, changes or additions. Include as much detail on why you're making the suggestion and any thinking towards a solution that you have already done.

### Navigation

In general all `.R` files will have a usable outline, so make use of that for navigation if in RStudio: `Ctrl-Shift-O`.

### Code styling 

The function `styler::style_dir()` will tidy code according to tidyverse styling using the styler package. Run this regularly as only tidied code will be allowed to be committed. This function also helps to test the running of the code and for basic syntax errors such as missing commas and brackets.

You should also run `lintr::lint_dir()` regularly as lintr will check all pull requests for the styling of the code, it does not style the code for you like styler, but is slightly stricter and checks for long lines, variables not using snake case, commented out code and undefined objects amongst other things.

---

### Contributing to the dashboard

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

---

### Contributing to the data pipelines

TBC


---

### Adding ad hoc scripts

Add some rules and structure to how we do this...

---

## Contact

explore.statistics@education.gov.uk