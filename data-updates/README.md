# Explore education statistics analytics - data updates

Some details here on the updates process, currently it's a manual process ran line by line by Laura

Ideally a diagram showing the pipelines, workflow schedule and database tables.

## Access requirements

If running yourself locally, or from your own auth, you will need a Google account with access to:
- Google Analytics
- Google Search Console
- ?

If you're running the code from within databricks, auth is already handled through our service account:
- shiny-app@ees-analytics.iam.gserviceaccount.com

To write or inspect the data created by the workflows you will need access to:
- catalog_40_copper_statistics_services

## Notebooks

Notebooks used in the scheduled databricks workflows are stored and tracked in `data-updates/notebooks/`. 

While you can write them from scratch, you can create a notebook in the editor in databricks itself and export the source (.r) file to start you off if easier.