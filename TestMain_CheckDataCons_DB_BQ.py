import psycopg2
from psycopg2 import sql
import psycopg2.extras as extras

from google.cloud import bigquery
from google.oauth2 import service_account


import pandas as pd
import json
from datetime import datetime,timezone

from dotenv import dotenv_values

import os
import sys 
from configupdater import ConfigUpdater


# create pandas series named view_source
view_source_data={'name':'pmr_project','app_key_name':'project_id','load_type':'merge'}
view_source_sr=pd.Series( data=view_source_data  )
print(view_source_sr)

# config = dotenv_values(dotenv_path=r"D:\PythonDev\MyQuantFinProject\SMartDataHub-DBToBigQuery\.env")
# print(config)


print("Get Configuration data")
view_name=view_source_sr['name']
key_id=view_source_sr['app_key_name']
load_type=view_source_sr['load_type']


cfg_path="cfg_last_import"
updater = ConfigUpdater()
updater.read(os.path.join(cfg_path,f"init_load.cfg"))

start_query=updater["metadata"]["xxxx"].value
print(f"Start Loading - UTC:{start_query}")


tz="utc"
dt_imported=datetime.now() # utc
str_date_imported=dt_imported.strftime('%d%m%Y_%H%M')
print(str_date_imported)


print("Config DB and BQ")

# In[40]:


env_path='.env'
config = dotenv_values(dotenv_path=env_path)
print(env_path)

bq_table_name=view_name.replace('pmr_','').replace("xyz_","")
print(f"{view_name} vs {bq_table_name}")


projectId=config["PROJECT_ID"]
credentials = service_account.Credentials.from_service_account_file(config["PROJECT_CREDENTIAL_FILE"])
client = bigquery.Client(credentials=credentials, project=projectId)
dw_dataset_id=config["MAIN_DATASET"]

dw_table_id = f"{projectId}.{dw_dataset_id}.{bq_table_name}"
print(dw_table_id)


print("Postgres & BigQuery")

# In[41]:


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
def list_data_pg(sql,params,connection): 
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

def load_data_bq(sql:str):

    query_result=client.query(sql)
    df_all=query_result.to_dataframe()
    return df_all


print("Get data from View on Postgres DB")

# In[42]:


def Get_ID_DB(): 
    sql_pg=f"""
    select {key_id} from {view_name} where updated_at AT time zone '{tz}' >= '{start_query}' 
    """
    print(sql_pg)
    df=list_data_pg(sql_pg,None,get_postgres_conn())
    return df
dfDB=Get_ID_DB()
dfDB.info()


print("Get data from Main table on BigQuery")



def Get_ID_BQ(): 
    if load_type=='merge':
        sql_bq=f""" SELECT {key_id} FROM `{dw_table_id}` WHERE  is_deleted=False """
    elif load_type =='bq-storage-api':
        sql_bq = f""" SELECT {key_id} FROM `{dw_table_id}` """
    else:
        raise Exception(f"Not found load type {load_type}")

    print(sql_bq)
    df=load_data_bq(sql_bq)
    return df

dfBQ=Get_ID_BQ()
dfBQ.info()


print("Comparison")

# In[44]:


def find_diff_id(dfPostgres,dfBigQuery): 
    """
    Find different ID betwee Postgresql and Bigquery
    Args:
    list1: Rows were returne from PostgresDB.
    list2: Rows were returne from Bigquery.

    Returns:
    A list of the values in BQ that are not in DB vice versa.

    """
    dbList=dfPostgres[key_id].tolist()
    bqList=dfBigQuery[key_id].tolist()

    # dbList=[1,2,3,4,5,6,7,8,9,10]
    # bqList=[1,2,3,4,5,10,11,12]
    print(f"DB:{len(dbList)} vs BQ:{len(bqList)}")

    if len(dbList)!=len(bqList):
        print(f"Not been sychronized to {dw_table_id} yet : list values in DB that are not in BQ")
        # diffDB=get_different_values(dbList,bqList)
        diffDB=  list(set(dbList)-set(bqList))
        print(diffDB)
        print("=================================================================================================")

        diffBQ=[]
        # print(f"Already deleted on {config['DATABASES_NAME']}: list values in BQ that are not in DB")
        # # diffBQ=get_different_values(bqList,dbList)
        # diffBQ=list(set(bqList)-set(dbList))
        # print(diffBQ)
        # return   diffDB,diffBQ
        return   diffDB,diffBQ

    else:
        print("Great")
        return [],[]



dbIDs,bqIDs=find_diff_id(dfDB,dfBQ)  


print("Get data from SMARTDB that have been synchoize to BigQuery")

# In[45]:


print("Get data from SMARTDB that have been synchoize to BigQuery")
def export_inconsistent_data(x_dbIDs,id): #Test
    if len(x_dbIDs)>0:
        x_dbIDs=[str(id) for id in x_dbIDs ]
        x_dbIDs="({})".format(",".join(x_dbIDs))
        print(x_dbIDs)
        sqlList=f"""
        select * from {view_name} where {id} in {x_dbIDs}
        order by updated_at desc
        """
        print(sqlList)
        dfXYZ=list_data_pg(sqlList,None,get_postgres_conn())
        if dfXYZ.empty==False:
            print(dfXYZ)
            dfXYZ.to_csv(f"data_consistence_check/{view_name}-{bq_table_name}_{str_date_imported}.csv",index=False)
            return True

        return False
    else:

        return False


result=export_inconsistent_data(dbIDs,key_id)
print(result)


# In[ ]:





    # In[ ]:




