# SMartDataHub-DBToBigQuery
Ingest data from [PostgreSQL](https://www.postgresql.org/) database that stores  data for [SMartApp](https://github.com/technqvi/SMartApp)  to [BigQuery](https://cloud.google.com/bigquery?hl=en) by capturing every transaction in the database periodically to maintain data integrity and consistency between Source(PostgreSQL) and Target(BigQuery). Please review how it works as figure and description below. Primarily, there are 2 steps the  as described below
<img width="1214" alt="process" src="https://github.com/technqvi/SMartDataHub-DBToBigQuery/assets/38780060/d61faef2-d0c8-4830-a72c-60323dc13d07">
### 1. Identifying and Capturing All Changed Data in a Django Database 
* Collect every changed data by object id of the content type (project,inventory,pm plan and pm item)  from models_logging_change table ([Django Models Logging](https://github.com/legion-an/django-models-logging)) to identify the actual action type such as added, deleted and chanaged status.
* Take list of all objectIds collected from previous step including action type to query data from transactional table (project,inventory,pm plan and pm item) .
### 2.1 Merge Solution To BigQuery (Sol1)
* Import data as dataframe to temporary table
* Run stored procedure to merge  data from temporary table to target table based on condition like added,changed or deleted status , if it is the deleted status the it is flagged as deleted in target table as opposed to actual delete.
* Truncate temporary table.

### 2.2 Bigquery Storage-API Solution to BigQuery(Sol2)
* Create  .proto file aligned with your data schema and compile file to .py to comply with Protocol Buffer.
* Read csv file as dataframe  and transform dataframe to get data ready for ingesting to Bigquery.
  * Convert datetime to timestamp as Microseconds.
  * Add _CHANGE_TYPE(action type) such as UPSERT,DELETE.
  * Fill null value with default value.
* Write JSON file from DataFrame such as Upsert file and Delete file.
* Write JSON data  as buffer protocol stream to BigQuery via  BigQuery Storage-API.



# Program Structure
* [LoadPGToBQ.py](https://github.com/technqvi/SMartDataHub-DBToBigQuery/blob/main/LoadPGToBQ.py)
* [LoadPGToBQ_BQStorageAPI.py](https://github.com/technqvi/SMartDataHub-DBToBigQuery/blob/main/LoadPGToBQ_BQStorageAPI.py)
* [smart_bq_storage_api](https://github.com/technqvi/SMartDataHub-DBToBigQuery/tree/main/smart_bq_storage_api)
* [CheckDataCons_DB_BQ.py](https://github.com/technqvi/SMartDataHub-DBToBigQuery/blob/main/CheckDataCons_DB_BQ.py) 
* [etl_web_admin](https://github.com/technqvi/SMartDataHub-DBToBigQuery/tree/main/etl_web_admin)
* [table_schema_script](https://github.com/technqvi/SMartDataHub-DBToBigQuery/tree/main/table_schema_script)
* [unittest](https://github.com/technqvi/SMartDataHub-DBToBigQuery/tree/main/unittest)
* [google_ai_py3.10.yml](https://github.com/technqvi/SMartDataHub-DBToBigQuery/blob/main/google_ai_py3.10.yml)


## References Solution
* Merge Table Solutions
  * [MERGE statement on Bigquery](https://cloud.google.com/bigquery/docs/using-dml-with-partitioned-tables#using_a_merge_statement)
  * [Merging data for Change Data Capture with GCP BigQuery](https://nileshk611.medium.com/change-data-capture-with-gcp-bigquery-6b09aec400bc) (Best Summarizaton)
* Bigquery Storage-API Solutions
  * [Announcing the public preview of BigQuery change data capture (CDC)](https://cloud.google.com/blog/products/data-analytics/bigquery-gains-change-data-capture-functionality)
  * [Stream table updates with change data capture](https://cloud.google.com/bigquery/docs/change-data-capture)
  * [Protocol Buffers](https://protobuf.dev/)
    * [InstallProtocol Buffers Compiler](https://github.com/protocolbuffers/protobuf/releases/tag/v25.1) 
    * [Protocol Buffers Python](https://github.com/protocolbuffers/protobuf/tree/main/python)
    * [Protocol Buffer Basics: Python](https://protobuf.dev/getting-started/pythontutorial/)
