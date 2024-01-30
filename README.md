# SMartDataHub-DBToBigQuery
Ingest data from [PostgreSQL](https://www.postgresql.org/) database that stores  data for [SMartApp](https://github.com/technqvi/SMartApp)  to [BigQuery](https://cloud.google.com/bigquery?hl=en) by capturing every transaction in the database periodically to keep data consistency and reliability. Please review how it works as figure and description below.
<img width="1214" alt="process" src="https://github.com/technqvi/SMartDataHub-DBToBigQuery/assets/38780060/d61faef2-d0c8-4830-a72c-60323dc13d07">
### Merge
### Bigquery Storage-API



# Program Structre


## References Solution
* Merge Solitons
  * [MERGE statement on Bigquery](https://cloud.google.com/bigquery/docs/using-dml-with-partitioned-tables#using_a_merge_statement)
  * [Mergeing data for Change Data Capture with GCP BigQuery](https://nileshk611.medium.com/change-data-capture-with-gcp-bigquery-6b09aec400bc)
* Bigquery Storage-API
  * [Announcing the public preview of BigQuery change data capture (CDC)](https://cloud.google.com/blog/products/data-analytics/bigquery-gains-change-data-capture-functionality)
  * [Stream table updates with change data capture](https://cloud.google.com/bigquery/docs/change-data-capture)
