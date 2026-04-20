# Databricks notebook source
# DBTITLE 1,Notebook description
# MAGIC %md
# MAGIC ## EES Public API - Service Summary App Table
# MAGIC
# MAGIC Creates a single aggregated app-layer table from the raw EES public API data ingested by the `raw_ees_api` notebook.
# MAGIC
# MAGIC One row per date, with a column for each API operation type (GET and POST endpoints).
# MAGIC
# MAGIC **Source**: `catalog_40_copper_statistics_services.analytics_raw.raw_ees_*`
# MAGIC
# MAGIC **Output**: `catalog_40_copper_statistics_services.analytics_app.ees_api_daily_queries`

# COMMAND ----------

# DBTITLE 1,Create ees_api_data_set_version_queries
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_40_copper_statistics_services.analytics_app.ees_api_daily_queries AS
# MAGIC WITH daily_by_type AS (
# MAGIC   -- top_level endpoint (GET)
# MAGIC   SELECT `_file_date` AS date, `type`, COUNT(*) AS total_queries
# MAGIC   FROM `catalog_40_copper_statistics_services`.`analytics_raw`.`raw_ees_top_level`
# MAGIC   GROUP BY `_file_date`, `type`
# MAGIC   UNION ALL
# MAGIC   -- publications endpoint (GET)
# MAGIC   SELECT `_file_date`,
# MAGIC     CASE WHEN `type` = 'GetSummary' THEN 'GetPublicationSummary' ELSE `type` END,
# MAGIC     COUNT(*)
# MAGIC   FROM `catalog_40_copper_statistics_services`.`analytics_raw`.`raw_ees_publications`
# MAGIC   GROUP BY `_file_date`, `type`
# MAGIC   UNION ALL
# MAGIC   -- data_sets endpoint (GET)
# MAGIC   SELECT `_file_date`,
# MAGIC     CASE WHEN `type` = 'GetSummary' THEN 'GetDataSetSummary' ELSE `type` END,
# MAGIC     COUNT(*)
# MAGIC   FROM `catalog_40_copper_statistics_services`.`analytics_raw`.`raw_ees_data_sets`
# MAGIC   GROUP BY `_file_date`, `type`
# MAGIC   UNION ALL
# MAGIC   -- data_set_versions endpoint (GET)
# MAGIC   SELECT `_file_date`,
# MAGIC     CASE WHEN `type` = 'GetSummary' THEN 'GetVersionSummary' ELSE `type` END,
# MAGIC     COUNT(*)
# MAGIC   FROM `catalog_40_copper_statistics_services`.`analytics_raw`.`raw_ees_data_set_versions`
# MAGIC   GROUP BY `_file_date`, `type`
# MAGIC   UNION ALL
# MAGIC   -- POST endpoint (query executions)
# MAGIC   SELECT `_file_date`, 'PostQueryExecutions', COUNT(*)
# MAGIC   FROM `catalog_40_copper_statistics_services`.`analytics_raw`.`raw_ees_query_access`
# MAGIC   GROUP BY `_file_date`
# MAGIC ),
# MAGIC pivoted AS (
# MAGIC   SELECT *
# MAGIC   FROM daily_by_type
# MAGIC   PIVOT (
# MAGIC     SUM(total_queries) FOR type IN (
# MAGIC       'GetPublications',
# MAGIC       'GetDataSets',
# MAGIC       'GetPublicationSummary',
# MAGIC       'GetDataSetSummary',
# MAGIC       'GetVersions',
# MAGIC       'DownloadCsv',
# MAGIC       'GetMetadata',
# MAGIC       'GetChanges',
# MAGIC       'GetVersionSummary',
# MAGIC       'PostQueryExecutions'
# MAGIC     )
# MAGIC   )
# MAGIC )
# MAGIC SELECT
# MAGIC   date,
# MAGIC   COALESCE(GetPublications, 0) AS GetPublications,
# MAGIC   COALESCE(GetDataSets, 0) AS GetDataSets,
# MAGIC   COALESCE(GetPublicationSummary, 0) AS GetPublicationSummary,
# MAGIC   COALESCE(GetDataSetSummary, 0) AS GetDataSetSummary,
# MAGIC   COALESCE(GetVersions, 0) AS GetVersions,
# MAGIC   COALESCE(DownloadCsv, 0) AS DownloadCsv,
# MAGIC   COALESCE(GetMetadata, 0) AS GetMetadata,
# MAGIC   COALESCE(GetChanges, 0) AS GetChanges,
# MAGIC   COALESCE(GetVersionSummary, 0) AS GetVersionSummary,
# MAGIC   COALESCE(PostQueryExecutions, 0) AS PostQueryExecutions
# MAGIC FROM pivoted
# MAGIC ORDER BY date;
