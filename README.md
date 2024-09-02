# SMartDataHub-DBToBigQuery
Ingest data from [PostgreSQL](https://www.postgresql.org/) database that stores  data for [SMartApp](https://github.com/technqvi/SMartApp)  to [BigQuery](https://cloud.google.com/bigquery?hl=en) by capturing every transaction in the database periodically to maintain data integrity and consistency between Source(PostgreSQL) and Target(BigQuery). Please review how it works as figure and description below. Primarily, there are 2 processes the  as described below
<img width="1214" alt="process" src="https://github.com/technqvi/SMartDataHub-DBToBigQuery/assets/38780060/0961dd7b-d1f6-42fa-89c3-93a47289822b">
<image src="data-integration.png">
### 1. Identifying and Capturing All Changed Data in a Django Database 
* Collect every changed data by object id of the content type (project,inventory,pm plan and pm item)  from models_logging_change table ([Django Models Logging](https://github.com/legion-an/django-models-logging)) to identify the actual action type such as added, deleted and chanaged status.
* Take list of all objectIds collected from previous step including action type to pull  data from transactional table (project,inventory,pm plan and pm item) .
### 2.1 Merge Solution To BigQuery (Sol1)
* Import data as dataframe to temporary table
* [Run stored procedure to merge data](https://github.com/technqvi/SMartDataHub-DBToBigQuery/blob/main/table_schema_script/3-pm_plan/merge_pm_plan.txt) from temporary table to target table based on action type condition  as below.
  * Added : run sql insert statement.
  * Changed : run sql update statement.
  * Deleted: run sql update statement to set only is_deleted culumn to be True as opposed to using sql delete statement.
* Truncate temporary table.

### 2.2 Bigquery Storage-API Solution to BigQuery(Sol2)
* Create  .proto file aligned with your data schema and compile file to .py to comply with Protocol Buffer.
* Read csv file as dataframe  and transform dataframe to get data ready for ingesting to Bigquery.
  * Convert datetime to timestamp as Microseconds.
  * Add _CHANGE_TYPE(action type) such as UPSERT,DELETE.
  * Fill null value with default value.
* Write JSON file from DataFrame such as Upsert file and Delete file.
* Write JSON data  as buffer protocol stream to BigQuery via  BigQuery Storage-API.

# Web Administration
<img width="926" alt="admin" src="https://github.com/technqvi/SMartDataHub-DBToBigQuery/assets/38780060/e2852cc3-163c-431e-bb50-e6ee20eadc89">



# Program Structure
* [LoadPGToBQ.py](https://github.com/technqvi/SMartDataHub-DBToBigQuery/blob/main/LoadPGToBQ.py) : Collect changed data for importing as dataframe to temp table on BigQuery.
* [LoadPGToBQ_BQStorageAPI.py](https://github.com/technqvi/SMartDataHub-DBToBigQuery/blob/main/LoadPGToBQ_BQStorageAPI.py) : convert dataframe to JSON file aligned with Protocal Buffer BigQuery Storage-API.
* [smart_bq_storage_api](https://github.com/technqvi/SMartDataHub-DBToBigQuery/tree/main/smart_bq_storage_api) : writte json file to BigQuery Storage-API.
* [CheckDataCons_DB_BQ.py](https://github.com/technqvi/SMartDataHub-DBToBigQuery/blob/main/CheckDataCons_DB_BQ.py) : Run test data consistency between PostgreSQL and BigQuery.
* [etl_web_admin](https://github.com/technqvi/SMartDataHub-DBToBigQuery/tree/main/etl_web_admin) : Web administration by Django to store table view configuration metadata and log ETL Transaction.
* [table_schema_script](https://github.com/technqvi/SMartDataHub-DBToBigQuery/tree/main/table_schema_script) : Script to create table , constraint  and view on Database and BigQuery including sample sql query.
* [unittest](https://github.com/technqvi/SMartDataHub-DBToBigQuery/tree/main/unittest) : Unit test for LoadPGToBQ.py and LoadPGToBQ_BQStorageAPI.py.
* [google_ai_py3.10.yml](https://github.com/technqvi/SMartDataHub-DBToBigQuery/blob/main/google_ai_py3.10.yml) : Create python anaconda envrionment for building this project. 


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
