# SMartDataHub-DBToBigQuery
Ingest data from [PostgreSQL](https://www.postgresql.org/) database that store  data for [SMartApp](https://github.com/technqvi/SMartApp)  to [BigQuery](https://cloud.google.com/bigquery?hl=en) by capturing every transaction in the database periodically for keeping data consistency. Please review how it works as figure and description below.
 <img width="1214" alt="process" src="https://github.com/technqvi/SMartDataHub-DBToBigQuery/assets/38780060/9807ceb9-fb0c-47b8-9015-37e668223dd0">






# References Solution
* Merge Solitons
  * [MERGE statement on Bigquery](https://cloud.google.com/bigquery/docs/using-dml-with-partitioned-tables#using_a_merge_statement)
  * [Mergeing data for Change Data Capture with GCP BigQuery](https://nileshk611.medium.com/change-data-capture-with-gcp-bigquery-6b09aec400bc)
* Bigquery Storage-API
  * [Announcing the public preview of BigQuery change data capture (CDC)](https://cloud.google.com/blog/products/data-analytics/bigquery-gains-change-data-capture-functionality)
  * [Stream table updates with change data capture](https://cloud.google.com/bigquery/docs/change-data-capture)
