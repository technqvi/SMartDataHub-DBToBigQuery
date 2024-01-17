#!/usr/bin/env python
# coding: utf-8

# # Imported Library

# In[1]:


import psycopg2
from psycopg2 import sql
import psycopg2.extras as extras
import pandas as pd
import json
from datetime import datetime,timezone
import time
from dateutil import tz

import os
import sys 

import pandas as pd
import numpy as np
from datetime import datetime 
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import BadRequest
import os
import sys 
import shutil

import CheckDataCons_DB_BQ as check_data  

import sqlite3

from configupdater import ConfigUpdater
# pip install ConfigUpdater

from dotenv import dotenv_values


# # Init value

# In[2]:


is_py=True
check_consistency=False
time_wait_for_bq=30
view_name = "pmr_pm_plan"


# In[3]:


isFirstLoad=False
                             

if is_py:
    press_Y=''
    ok=False

    if len(sys.argv) > 1:
        view_name=sys.argv[1]
    else:
        print("Enter the following input: ")
        view_name = input("View Table Name : ")
print(f"View name to load to BQ :{view_name}")


# # Imported date

# In[4]:


dt_imported=datetime.now(timezone.utc) # utc
dt_imported=datetime.strptime(dt_imported.strftime("%Y-%m-%d %H:%M:%S"),"%Y-%m-%d %H:%M:%S")
print(f"UTC: {dt_imported} For This Import")

str_dt_imported=dt_imported.strftime("%Y-%m-%d %H:%M:%S")


# # Read Configuration File 

# In[5]:


# Test config,env file and key to be used ,all of used key  are existing.
cfg_path="cfg_last_import"
env_path='.env'

updater = ConfigUpdater()
updater.read(os.path.join(cfg_path,f"{view_name}.cfg"))

config = dotenv_values(dotenv_path=env_path)

data_base_file="etl_web_admin/bq_cdc_etl_transaction.db"

print(env_path)
print(cfg_path)


# In[6]:


log = "models_logging_change"
data_name=view_name.replace("pmr_","").replace("xyz_","")
sp_name=f"merge_{data_name}"


# # SQLite

# In[7]:


sqlite3.register_adapter(np.int64, lambda val: int(val))
sqlite3.register_adapter(np.int32, lambda val: int(val))


def list_data_sqlite(sql):
    conn = sqlite3.connect(os.path.abspath(data_base_file))
    print(sql)
    df_item=pd.read_sql_query(sql, conn)
    return df_item

def addETLTrans(recordList):
    try:
        sqliteConnection = sqlite3.connect(os.path.abspath(data_base_file))
        cursor = sqliteConnection.cursor()
        sqlite_insert_query = """
        INSERT INTO etl_transaction
        (trans_datetime, view_source_id,no_rows,is_consistent,is_complete)  
        VALUES (?,?,?,?,?);
         """
        cursor.executemany(sqlite_insert_query, recordList)
        print("Done ETL Trasaction")
        sqliteConnection.commit()
        cursor.close()

    except Exception as e:
        print("Failed to insert etl_transaction table", str(e))
    finally:
        if sqliteConnection:
            sqliteConnection.close()

    


# # Get View Source  to set configuration data

# In[8]:


def get_view_source(name):
    sql=f"select * from view_source where name='{name}' limit 1"
    dfView=list_data_sqlite(sql)
    if dfView.empty==False:
       view_source=dfView.iloc[0,:]
    else:
        error=f"Not found {view_name} view"
        raise Exception(error)
    return view_source
view_source= get_view_source(view_name)
print(view_source)


# In[10]:


admin_view_id=view_source['id']
content_id=view_source['app_conten_type_id']
view_name_id=view_source['app_key_name']


changed_field_mapping=view_source['app_changed_field_mapping'].strip().split(",")
changed_field_mapping = [ x.replace(" ", "").replace("\r", "").replace("\n", "") for x  in changed_field_mapping] 

way=view_source['load_type'] # 1="merge"  or "bq-storage-api"

pk_fk_list=[]
if view_source['app_fk_name_list'] is not None:
    pk_fk_list=view_source['app_fk_name_list'].strip().split(",")
    pk_fk_list= [ x.replace(" ", "").replace("\r", "").replace("\n", "") for x  in  pk_fk_list] 
pk_fk_list.append(view_name_id)

print(f"LoadyType:{way} # ContentyTypeID:{content_id} # KeyName:{view_name_id} # SP:{sp_name}")
print(changed_field_mapping)
print(pk_fk_list)


# # BigQuery Configuration

# In[11]:


# Test exsitng project dataset and table anme

projectId=config['PROJECT_ID']  # smart-data-ml  or kku-intern-dataai or ponthorn
credential_file=config['PROJECT_CREDENTIAL_FILE']
# C:\Windows\smart-data-ml-91b6f6204773.json
# C:\Windows\kku-intern-dataai-a5449aee8483.json
# C:\Windows\pongthorn-5decdc5124f5.json


dataset_id=config['TEMP_DATASET'] # 'SMartData_Temp'  'PMReport_Temp'
main_dataset_id=config['MAIN_DATASET']  # ='SMartDataAnalytics'  'PMReport_Main'

credentials = service_account.Credentials.from_service_account_file(credential_file)

table_name=f"temp_{data_name}" #can change in ("name") to temp table
table_id = f"{projectId}.{dataset_id}.{table_name}"
print(table_id)


main_table_name=data_name
main_table_id = f"{projectId}.{main_dataset_id}.{main_table_name}"
print(main_table_id)

# https://cloud.google.com/bigquery/docs/reference/rest/v2/Job
to_bq_mode="WRITE_EMPTY"


client = bigquery.Client(credentials= credentials,project=projectId)


# # Check configuration parameter validation:

# # Get Last Import to retrive data after that

# In[12]:


last_imported=datetime.strptime(updater["metadata"][view_name].value,"%Y-%m-%d %H:%M:%S")
print(f"{data_name} - UTC:{last_imported}  Of Last Import")

# local_zone = tz.tzlocal()
# last_imported = last_imported.astimezone(local_zone)
# print(f"Local Asia/Bangkok:{last_imported}")


# # Postgres &BigQuery

# In[13]:


def get_postgres_conn():
 try:
  conn = psycopg2.connect(
        database=config['DATABASES_NAME'], user=config['DATABASES_USER'],
      password=config['DATABASES_PASSWORD'], host=config['DATABASES_HOST']
     )
  return conn

 except Exception as error:
  print(error)      
  raise error
def list_data(sql,params,connection):
 df=None   
 with connection.cursor() as cursor:
    
    if params is None:
       cursor.execute(sql)
    else:
       cursor.execute(sql,params)
    
    columns = [col[0] for col in cursor.description]
    dataList = [dict(zip(columns, row)) for row in cursor.fetchall()]
    df = pd.DataFrame(data=dataList) 
 return df 




# In[14]:


def get_bq_table():
 try:
    table=client.get_table(table_id)  # Make an API request.
    print("Table {} already exists.".format(table_id))
    print(table.schema)
    return True
 except NotFound:
    raise Exception("Table {} is not found.".format(table_id))
    

def insertDataFrameToBQ(df_trasns):
    try:
        job_config = bigquery.LoadJobConfig(write_disposition=to_bq_mode,)
        job = client.load_table_from_dataframe(df_trasns, table_id, job_config=job_config)
        try:
         job.result()  # Wait for the job to complete.
        except ClientError as e:
         print(job.errors)

        print("Total ", len(df_trasns), f"Imported data to {table_id} on bigquery successfully")

    except BadRequest as e:
        print("Bigquery Error\n")
        print(e) 


# # Check Data Consistency

# In[15]:


def do_check_consistency():
    check_result=True
    if check_consistency:
         print("Wait in a while for biqguery to update")
         time.sleep(time_wait_for_bq)
         print("Check data consistency betwwen database and bigquery")
         result=check_data.check_data_consistency_db_bq(view_name)
         if result:
            print("if result=True , view csv file in check_db_bq  data_consistence_check")  
            print("send email to admin to investigate somthing wrong.")
            check_result=False
         else:
            print(f"Data has been consistent between {config['DATABASES_NAME']} and {main_table_id}")
    else:
        print("Disable checking data consistency feature.")
            
    return int(check_result)


# # Check whether it is the first loading?

# In[16]:


def checkFirstLoad():
    print("If the main table is empty , so the action of each row  must be 'added' on temp table")
    rows_iter   = client.list_rows(main_table_id, max_results=1) 
    no_main=len(list(rows_iter))
    if no_main==0:
     isFirstLoad=True
     print(f"This is the first loaing , so there is No DATA in {main_table_id}, we load all rows from {view_name} to import into {table_id} action will be 'added' ")
    else:
     isFirstLoad=False   
    return isFirstLoad


# In[17]:


isFirstLoad=checkFirstLoad()
print(f"IsFirstLoad={isFirstLoad} for {data_name}")


# # For The next Load
# * Get data from model log based on condition last_imported and table
# * Get all actions from log table by selecting unique object_id and setting by doing something as logic
# * Create  id and action dataframe form filtered rows from log table

# In[18]:


def list_model_log(x_last_imported,x_content_id):
    sql_log = f"""
    SELECT object_id, action,TO_CHAR(date_created,'YYYY-MM-DD HH24:MI:SS') as date_created ,changed_data
    FROM {log}
    WHERE date_created  AT time zone 'utc' >= '{x_last_imported}' AND content_type_id = {x_content_id} 
    ORDER BY object_id, date_created
    """
    print(sql_log)


    # Asia/Bangkok 
    lf = list_data(sql_log, None, get_postgres_conn())
    print(f"Retrieve all rows after {last_imported}")
    print(lf.info())
    return lf


# # Find Change in Mappping

# In[19]:


def findChangeInListMapping(changed_data):
    # print(type(changed_data))
    # print(changed_data)
    x=False
    for key in changed_data.keys():
        # print(key)
        if key in changed_field_mapping :
            print(f"{key} in {changed_field_mapping}")
            x= True

    return x
    

def check_no_changes_to_columns_view_only_changed_action(dfAction,x_view_name,_x_key_name):
    """
    Check dataframe from log model that contain only changed action to select changed fields on view.
    Gather id no any changes based on  changed_field_mapping to get rid of it from list to import to BQ
    """

    listACtion=dfAction["action"].unique().tolist()
    if len(listACtion)==1 and listACtion[0]=='changed':
        print("#######################Find Some Changes#############################")
        print("Process dataframe containing only all changed action")
        dfAction['x']=dfAction['changed_data'].apply(findChangeInListMapping)
        print(dfAction[['object_id','x','changed_data']])
        
        any_rows_match = dfAction['x'] ==True
        match_x=any_rows_match.any()
        print("Check whether at least one row in a DataFrame matches a specific criteria")
        print(match_x)
        # there is at least one change in mapping changed_field_mapping : return false
        if match_x:
            return False
        # there is no any change in mapping changed_field_mapping : return true  
        else: # return to caller for deleteing from list
            return True
        print("#####################################################################")
    else:
        return False
        
    


# In[20]:


listForRemove=[]
def select_actual_action(lf):
    listIDs=lf["object_id"].unique().tolist()
    listUpdateData=[]
    for id in listIDs:
        lfTemp=lf.query("object_id==@id")
        print(f"--------------------{id}---------------------------------")
        print(lfTemp)
        print(f"--------------------end---------------------------------")
        
        
        
        x=check_no_changes_to_columns_view_only_changed_action(lfTemp,view_name,view_name_id)
        if x==True:
           print(f"RemoveID {id}") 
           listForRemove.append(id) 


        first_row = lfTemp.iloc[0]
        last_row = lfTemp.iloc[-1]
        # print(first_row)
        # print(last_row)

        if len(lfTemp)==1:
            listUpdateData.append([id,first_row["action"]])
        else:
            if first_row["action"] == "added" and last_row["action"] == "deleted":
                continue
            elif first_row["action"] == "added" and last_row["action"] != "deleted":
                listUpdateData.append([id,"added"])
            else : listUpdateData.append([id,last_row["action"]])

    print("Convert listUpdate to dataframe")
    dfUpdateData = pd.DataFrame(listUpdateData, columns= ['id', 'action'])
    dfUpdateData['id'] = dfUpdateData['id'].astype('int64')
    dfUpdateData=dfUpdateData.sort_values(by="id")
    dfUpdateData=dfUpdateData.reset_index(drop=True)

    return dfUpdateData


# In[21]:


if isFirstLoad==False:
    listModelLogObjectIDs=[]
    dfModelLog=list_model_log(last_imported,content_id)
    if dfModelLog.empty==True:

        dfTran=pd.DataFrame(data={
        "trans_datetime":[str_dt_imported],"view_source_id":[admin_view_id],
        "no_rows":[0],"is_consistent":[do_check_consistency()],"is_complete":[1]
        } )
        addETLTrans(dfTran.to_records(index=False) )
        print("No row to be imported.")
        exit()
    else:
        print("Get row imported from model log to set action") 
        dfModelLog=select_actual_action( dfModelLog)
        listForRemove=[int(id) for id in listForRemove ]
        print(f"Remove these Ids from dfModelLog : {listForRemove}")
        dfModelLog=dfModelLog.query("id not in @listForRemove")
        listModelLogObjectIDs=dfModelLog['id'].tolist()

        print(dfModelLog.info())
        print(dfModelLog)       
        print(listModelLogObjectIDs) 
 


# In[ ]:





# # Load view and transform

# In[22]:


def retrive_next_data_from_view(x_view,x_id,x_listModelLogObjectIDs):
    if len(x_listModelLogObjectIDs)>1:
     sql_view=f"select *  from {x_view}  where {x_id} in {tuple(x_listModelLogObjectIDs)}"
    else:
     sql_view=f"select *  from {x_view}  where {x_id} ={x_listModelLogObjectIDs[0]}"
    
    print(sql_view)
    df=list_data(sql_view,None,get_postgres_conn())

    if df.empty==True:
     return df
    df=df.drop(columns='updated_at')
    return df 


def retrive_first_data_from_view(x_view,x_last_imported):
     sql_view=f"select *  from {x_view}  where  updated_at AT time zone 'utc' >= '{x_last_imported}'"
     print(sql_view)
     df=list_data(sql_view,None,get_postgres_conn())
     if df.empty==True:
            return df
     df=df.drop(columns='updated_at')
     df['action']='added'
     return df   
def retrive_one_row_from_view_to_gen_df_schema(x_view):
    sql_view=f"select *  from {x_view}  limit 1"
    print(sql_view)
    df=list_data(sql_view,None,get_postgres_conn())
    df=df.drop(columns='updated_at')
    return df


if isFirstLoad:
 df=retrive_first_data_from_view(view_name,last_imported)
 if df.empty==True:
    # create dataframe and addETLTrans 0 row    
    print("No row to be imported.")
    exit()
 else:
    print(df)

else:
 df=retrive_next_data_from_view(view_name,view_name_id,listModelLogObjectIDs)  
 if df.empty==True:
    print("Due to having deleted items, we will Get schema from {} to create empty dataframe with schema.")
    df=retrive_one_row_from_view_to_gen_df_schema(view_name)
    # this id has been included in listModelLogObjectIDs which contain deleted action , so we can use it as schema generation
    print(df)

    
print(df.info())    
    


# # Data Transaformation
# * IF The first load then add actio='Added'
# * IF The nextload then Merge LogDF and ViewDF and add deleted row 
#   * Get Deleted Items  to Create deleted dataframe by using listDeleted
#   * If there is one deletd row then  we will merge it to master dataframe
# * IF the next load has only deleted action

# In[23]:


def add_acutal_action_to_df_at_next(df,dfUpdateData,x_view,x_id):
    # merget model log(id and action) to data view
    # if  dfUpdateData  contain only deleted action
    # we will merge to get datafdame shcema, it can perform inner without have actual data fram view
    merged_df = pd.merge(df, dfUpdateData, left_on=view_name_id, right_on='id', how='inner')
    merged_df = merged_df.drop(columns=['id'])

    listAllAction=dfUpdateData['id'].tolist()
    print(f"List {listAllAction} all action")
    
    listSeleted = merged_df[view_name_id].tolist()
    print(f"List  {x_view}   {listSeleted} from {x_view} exluding deleted action")
    
    allActionSet = set(listAllAction)
    anotherSet = set(listSeleted)
    
    listDeleted = list(allActionSet.symmetric_difference(anotherSet))
    print(f"List deleted {listDeleted}")
    
    # Test List  select by view + List deeleted = List All Action

    if len(listDeleted)>0:
        print("There are some deleted rows")
        dfDeleted=pd.DataFrame(data=listDeleted,columns=[view_name_id])
        dfDeleted['action']='deleted'
        print(dfDeleted)
        merged_df=pd.concat([merged_df,dfDeleted],axis=0)

    else:
        print("No row deleted")

    return merged_df    




# In[24]:


if isFirstLoad==False:
 df=add_acutal_action_to_df_at_next(df,dfModelLog,view_name,view_name_id)
print(df)


# In[ ]:





# # Last Step :Check duplicate ID & reset index & convert all pk&fk to int64

# In[ ]:





# In[27]:


hasDplicateIDs = df[view_name_id].duplicated().any()
if  hasDplicateIDs:
 raise Exception("There are some duplicate id on dfUpdateData")
else:
 print(f"There is no duplicate {view_name_id} ID")  

if len(pk_fk_list)>0:
 df[pk_fk_list] = df[pk_fk_list].astype('Int64')

# merged_df['imported_at']=dt_imported
df=df.reset_index(drop=True  )
print(df.info())
print(df)


# In[ ]:





# # Insert data to BQ data frame & # Run StoreProcedure To Merge Temp&Main and Truncate Transaction 

# In[28]:


if way=='merge':
    print("1#Ingest data into Bigquery")
    if get_bq_table():
        try:
            insertDataFrameToBQ(df)
        except Exception as ex:
            raise ex
            
    print("2#Run StoreProcedure To Merge Temp&Main and Truncate Transaction.")
    # https://cloud.google.com/bigquery/docs/transactions
    sp_id_to_invoke=f""" CALL `{projectId}.{main_dataset_id}.{sp_name}`() """
    print(sp_id_to_invoke)    
    sp_job = client.query(sp_id_to_invoke)

else:
    df.to_csv(f"bq_storage_api/{data_name}_{way}.csv",index=False)
    # invoke ingest_data_bq_storage_api


# 

# 
# # Update New Recenet Update to file
# 

# In[29]:


updater["metadata"][view_name].value=dt_imported.strftime("%Y-%m-%d %H:%M:%S")
updater.update_file() 


# In[30]:


print(datetime.now(timezone.utc) )


# # Add ETL transaction

# In[31]:


print("Add ETLTrans n-row as dataframe")   

dfTran=pd.DataFrame(data={
"trans_datetime":[str_dt_imported],"view_source_id":[admin_view_id],
 "no_rows":[len(df)],"is_consistent":[do_check_consistency()],"is_complete":[1]
} )
addETLTrans(dfTran.to_records(index=False) )


# In[ ]:





# In[ ]:




