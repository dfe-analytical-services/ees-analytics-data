# Explore education statistics analytics - data updates

Analytics data for EES is updated daily using a series of databricks notebooks in an 'EES analytics update' workflow, code for these workflows is in `notebooks/`.

There is a workflow in databricks that runs a number of jobs, each job is defined in a separate notebook.

To understand how all the pieces fit together, look at the [high level design and database map for the data processing flow (LucidSpark)](https://lucid.app/lucidchart/97ee2663-4065-425e-92df-dd664d44973d/edit?viewport_loc=-377%2C83%2C2432%2C1203%2CKh6Qkubd_WT.&invitationId=inv_1289a047-b729-46bc-85ef-425229b540a5).

## Access requirements

The code in the notebooks folder is designed to be ran from within databricks itself.

If you're running the code from within databricks, auth is already handled through our service account:
- shiny-app@ees-analytics.iam.gserviceaccount.com

To write or inspect the data created by the workflows you will need access to:
- catalog_40_copper_statistics_services

If running any code yourself locally, or from your own auth, you will need a Google account with access to:
- Google Analytics
- Google Search Console

Also note that the renv.lock file in the root is not used for these workflows, instead they install their own dependencies.

## Notebooks

Notebooks used in the scheduled databricks workflows are stored and tracked in `data-updates/notebooks/`. Common variables and functions for the notebooks are stored in `notebooks/utils.R`.

While you can write them from scratch, you can create a notebook in the editor in databricks itself and export the source (.r) file to start you off if easier.

Broadly we have two kinds of notebooks:

1. Ingest raw data
    - These notebooks start with the prefix `raw_*` and connect to various sources to pull in new data and append to existing tables. 
    - These tables will recognise when the most recent data available is and only query the relevant APIs for that data.
    - These are handled carefully with several checks before writing the new tables, if we lose the tables we lose our data. Due to retention periods (e.g. GA), these tables can not be fully recreated if lost.

2. Process app data
    - These notebooks start with the prefix `app_*` and are used in the [EES analytics dashboard](https://github.com/dfe-analytical-services/ees-analytics-dashboard).
    - As the app tables are purely for use in the analytics app and can always be rebuilt from the raw tables, these notebooks will usually just rebuild the whole table from scratch each time.
    - There are quality checks, usually if they fail you might just have an edge case in the analytics data we've not anticipated that you will need to account for in the relevant `app_*` notebook.

The `notebooks/ees_last_updated.r` notebook is used to trigger updates of the last updated date table in the database. This is key (literally), as it's used as a cache key in the app to tell it when to re-run larger data queries.

Currently, as this process was built at speed, it is suboptimal. Most of the notebooks use very similar repetitive code and are ran linearly in the EES analytics update (LINEAR) workflow. In future, to make maintenance easier the code should be consolidated to use more commmon functions / notebooks and also to run in parallel, an example of how parallel running might look, showing dependencies between notebooks is visible in the original EES analytics update workflow.
