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

The `notebooks/ees_last_updated.r` notebook is used to trigger updates of the last updated date table in the database. This is key (literally), as it's used as a cache key in the app to tell it when to re-run larger data queries.
