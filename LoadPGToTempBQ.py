#!/usr/bin/env python
# coding: utf-8

# # Imported Library

# In[68]:


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


import bq_storage_api.incident_data_pb2 as pb2_incident



# In[69]:


check_consistency=True
time_wait_for_bq=60
is_py=True

# pmr_ for merg and xyz_ for bq-storage-api
way="merge" # 1="merge"  or "bq-storage-api"
view_name = "pmr_project"

# way="merge" 
# view_name = "pmr_pm_plan"


# In[70]:


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

# In[71]:


dt_imported=datetime.now(timezone.utc) # utc
dt_imported=datetime.strptime(dt_imported.strftime("%Y-%m-%d %H:%M:%S"),"%Y-%m-%d %H:%M:%S")
print(f"UTC: {dt_imported} For This Import")

str_dt_imported=dt_imported.strftime("%Y-%m-%d %H:%M:%S")


# # Set view data and log table and protocolbuffers

# In[72]:


log = "models_logging_change"
data_name=view_name.replace("pmr_","").replace("xyz_","")
data_pb2=None
sp=f"merge_{data_name}"

def get_process_configuration_data(view_name):
    
    x_data_pb2=None
    if view_name == "pmr_pm_plan":
        tableContentID = 36
        key_name = "pm_id"
        changed_field_mapping=['planned_date','ended_pm_date',
                               'project_id','remark','team_lead_id']

    elif view_name == "pmr_pm_item":
        tableContentID = 37
        key_name = "pm_item_id"

    elif view_name == "pmr_project":
        tableContentID = 7
        key_name = "project_id"

    elif view_name == "pmr_inventory":
        tableContentID = 14
        key_name = "inventory_id"
        
    elif view_name == "xyz_incident":
        tableContentID = 18
        key_name = "incident_id"   
        x_data_pb2=pb2_incident.IncidentData()

    else:
        raise Exception("No specified content type id")
        
    return tableContentID, key_name,sp,x_data_pb2


                             
                               
content_id , view_name_id,sp_name,x_data_pb2=get_process_configuration_data(view_name)
print(content_id," - ",view_name_id)


# In[ ]:





# # Check configuration parameter validation:

# In[73]:


#  # 1="merge"  or "bq-storage-api"
def check_config_parameter_validation(way,sp,data_pb2):
  if  (way=="merge") and sp is None:
     raise Exception(f"StoreProcedure is not allowed to None in {way} Way.")
  elif  (way=="bq-storage-api") and data_pb2 is None:
     raise Exception(f"ProtoBuf Data is not allowed to None in {way} Way.")   
  return True
    
result_data_validation=check_config_parameter_validation(way,sp,data_pb2)
if result_data_validation and  way=="merge":
 print(f"{way} - {sp}")
elif result_data_validation and  way=="bq-storage-api":
 print(f"{way}")
 print(data_pb2.DESCRIPTOR)


# # Set data and cofig path

# In[74]:


# Test config,env file and key to be used ,all of used key  are existing.
cfg_path="cfg_last_import"
env_path='.env'

updater = ConfigUpdater()
updater.read(os.path.join(cfg_path,f"{view_name}.cfg"))

config = dotenv_values(dotenv_path=env_path)

data_base_file="etl_web_admin/bq_cdc_etl_transaction.db"

print(env_path)
print(cfg_path)


# In[75]:


# Test exsitng project dataset and table anme

projectId=config['PROJECT_ID']  # smart-data-ml  or kku-intern-dataai or ponthorn
credential_file=config['PROJECT_CREDENTIAL_FILE']
# C:\Windows\smart-data-ml-91b6f6204773.json
# C:\Windows\kku-intern-dataai-a5449aee8483.json
# C:\Windows\pongthorn-5decdc5124f5.json


dataset_id='SMartData_Temp'  # 'SMartData_Temp'  'PMReport_Temp'
main_dataset_id='SMartDataAnalytics'  # ='SMartDataAnalytics'  'PMReport_Main'

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


# Read Configuration File and Initialize BQ Object

# In[76]:


last_imported=datetime.strptime(updater["metadata"][view_name].value,"%Y-%m-%d %H:%M:%S")
print(f"{data_name} - UTC:{last_imported}  Of Last Import")

# local_zone = tz.tzlocal()
# last_imported = last_imported.astimezone(local_zone)
# print(f"Local Asia/Bangkok:{last_imported}")


# # Postgres &BigQuery & SQLite

# In[77]:


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




# In[78]:


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
        (trans_datetime, view_source_id,type,no_rows,is_consistent,is_complete)  
        VALUES (?,?,?,?,?,?);
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

    


# In[79]:


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


# # Get View Source

# In[80]:


def get_view_source(name):
    sql=f"select * from view_source where name='{name}' limit 1"
    dfView=list_data_sqlite(sql)
    if dfView.empty==False:
       view_source_id=dfView.iloc[0,0]
    else:
        error=f"Not found {view_name} view"
        raise Exception(error)
    return view_source_id
admin_view_id= get_view_source(view_name)
print(admin_view_id)


# # Check Data Consistency

# In[111]:


def do_check_consistency():
    check_result=True
    if check_consistency:
         print("Wait in a while for biqguery to update")
         time.sleep(time_wait_for_bq)
         print("Check data consistency betwwen database and bigquery")
         result=check_data.check_data_consistency_db_bq(view_name)
         if result:
            print("if result=True , view csv file in check_db_bq  data_consistence_check")  
            print("send email to admin to investigate")
            check_result=False
         else:
            print(f"Data has been consistent between {config['DATABASES_NAME']} and {main_table_id}")
    else:
        print("Disable checking data consistency feature.")
            
    return int(check_result)


# # Check whether it is the first loading?

# In[82]:


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


# In[83]:


isFirstLoad=checkFirstLoad()
print(f"IsFirstLoad={isFirstLoad} for {data_name}")


# # For The next Load
# * Get data from model log based on condition last_imported and table
# * Get all actions from log table by selecting unique object_id and setting by doing something as logic
# * Create  id and action dataframe form filtered rows from log table

# In[84]:


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


# In[85]:


def check_any_changes_to_collumns_view(dfAction,x_view_name,_x_key_name):
    """
    Check dataframe from log model that contain only changed action to select changed fields on view.
    """

    listACtion=dfAction["action"].unique().tolist()
    if len(listACtion)==1 and listACtion[0]=='changed':
        print("###########################################################")
        print("Process dataframe containing only all changed action")
        print(dfAction)
        print("###########################################################")
    
    


# In[86]:


def select_actual_action(lf):
    listIDs=lf["object_id"].unique().tolist()
    listUpdateData=[]
    for id in listIDs:
        lfTemp=lf.query("object_id==@id")
        print(f"--------------------{id}---------------------------------")
        print(lfTemp)
        
        
        
        # check_any_changes_to_collumns_view(lfTemp,content_id,view_name_id)


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


# In[87]:


if isFirstLoad==False:
    listModelLogObjectIDs=[]
    dfModelLog=list_model_log(last_imported,content_id)
    if dfModelLog.empty==True:
            
        dfTran=pd.DataFrame(data={
        "trans_datetime":[str_dt_imported],"view_source_id":[admin_view_id],
        "type":[way],"no_rows":[0],"is_consistent":[do_check_consistency()],"is_complete":[1]
        } )
        addETLTrans(dfTran.to_records(index=False) )
        
        print("No row to be imported.")
        
        exit()
    else:
       print("Get row imported from model log to set action") 
       dfModelLog=select_actual_action( dfModelLog)
       listModelLogObjectIDs=dfModelLog['id'].tolist()
       print(dfModelLog.info())
       print(dfModelLog)       
       print(listModelLogObjectIDs) 


# # Load view and transform

# In[88]:


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

# In[89]:


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




# In[90]:


if isFirstLoad==False:
 df=add_acutal_action_to_df_at_next(df,dfModelLog,view_name,view_name_id)

print(df)


# In[ ]:





# # Last Step :Check duplicate ID & reset index

# In[91]:


hasDplicateIDs = df[view_name_id].duplicated().any()
if  hasDplicateIDs:
 raise Exception("There are some duplicate id on dfUpdateData")
else:
 print(f"There is no duplicate {view_name_id} ID")  


# merged_df['imported_at']=dt_imported
df=df.reset_index(drop=True  )
print(df.info())
print(df)


# In[ ]:





# # Insert data to BQ data frame & # Run StoreProcedure To Merge Temp&Main and Truncate Transaction 

# In[92]:


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
    bq_storage_api_path="bq_storage_api"
    df.to_csv(f"{data_name}_{way}.csv",index=False)


# # BQ-Storage-API Data Transformation

# In[93]:


# df


# In[94]:


# from google.protobuf.timestamp_pb2 import Timestamp
# print("add timestamp import")
# dtimestamp = Timestamp()
# dtimestamp.FromDatetime(dt_imported)
# update_at_micro_timestampe =dtimestamp.ToMicroseconds()
# df['update_at']=update_at_micro_timestampe 


# In[ ]:





# In[95]:


# print("change action type")

# def change_action_merge_to_bq_storage_api(x):
#     if x=="added" or x=="changed":
#         return  "UPSERT"
#     else:
#         return "DELETE"

    
# df["_CHANGE_TYPE"]=df['action'].apply(change_action_merge_to_bq_storage_api)
# df=df.drop(columns=['action'])


# # Split data into Upsert and Delete

# In[96]:


# dfUpsert=df.query("_CHANGE_TYPE=='UPSERT'")
# dfUpsert


# In[97]:


# dfDelete=df.query("_CHANGE_TYPE=='DELETE'")
# dfDelete


# ## if you convert any time of any tz to timestampe for converting to Microseconds , it wll turn into UTC 
# * to_char((abc.incident_datetime AT TIME ZONE 'Asia/Bangkok'::text),
#            'YYYY-MM-DD HH24:MI'::text)   AS open_datetime
# * to_char((abc.incident_datetime AT TIME ZONE 'UTC'::text),
#                'YYYY-MM-DD HH24:MI'::text)   AS open_datetime
# * https://www.epochconverter.com/
# 
# ## DateTime is UTC

# ### null dattime is replaced with 0(GMT:1-1-1970 12:00:00 AM)

# In[98]:


# if dfUpsert.empty==False:
#     print("convert strng to datetime and microseconds")
#     from google.protobuf.timestamp_pb2 import Timestamp
#     def convert_string_to_datetime_timestamp_microseconds (dt_str):
#         if dt_str is not None:   
#             dt=datetime.strptime(dt_str,"%Y-%m-%d %H:%M")
#             # return dt
#             x_timestamp = Timestamp()
#             x_timestamp.FromDatetime(dt)
#             micro_x =x_timestamp.ToMicroseconds()
#             return micro_x
#         else:
#             None
#     #        
#     datetimeCols=["open_datetime","close_datetime"]
#     for d in datetimeCols:
#         # check whick column contain null value if so, convert float64 to int 32
#         dfUpsert[d]=dfUpsert[d].apply(convert_string_to_datetime_timestamp_microseconds)
#         dfUpsert[d] = dfUpsert[d].fillna(0)
#         dfUpsert[d]=dfUpsert[d].astype('Int64')


# In[99]:


# if dfDelete.empty==False:
#     dfDelete=dfDelete[[view_name_id,"_CHANGE_TYPE"]]
    


# # Write Json File

# In[100]:


# df['inventory_id']

# if  dfUpsert.empty==False:
#     json_file="incident_upsert.json"
#     json_file_path=os.path.join(bq_storage_api_path,json_file)

#     json_incident_data = json.loads(dfUpsert.to_json(orient = 'records'))
#     with open(json_file_path, "w") as outfile:
#         json.dump(json_incident_data, outfile)
# print(dfUpsert.info())
# dfUpsert


# In[101]:


# if  dfDelete.empty==False:
#     json_file="incident_delete.json"
#     json_file_path=os.path.join(bq_storage_api_path,json_file)
#     json_incident_data = json.loads(dfDelete.to_json(orient = 'records'))
#     with open(json_file_path, "w") as outfile:
#         json.dump(json_incident_data, outfile)
# print(dfDelete.info())
# dfDelete


# In[102]:


# delete json file if successful


# 

# 

# 
# # Update New Recenet Update to file
# 

# In[103]:


updater["metadata"][view_name].value=dt_imported.strftime("%Y-%m-%d %H:%M:%S")
updater.update_file() 


# In[104]:


print(datetime.now(timezone.utc) )


# # Add ETL transaction

# In[106]:


print("addETLTrans n-row as dataframe")   

dfTran=pd.DataFrame(data={
"trans_datetime":[str_dt_imported],"view_source_id":[admin_view_id],
"type":[way],"no_rows":[len(df)],"is_consistent":[do_check_consistency()],"is_complete":[1]
} )
addETLTrans(dfTran.to_records(index=False) )


# In[ ]:





# In[ ]:




