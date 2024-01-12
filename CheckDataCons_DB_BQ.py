#!/usr/bin/env python
# coding: utf-8

# In[77]:


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


# In[ ]:





# In[78]:


is_py=True
view_name='pmr_pm_item'

start_query="2019-01-01"

tz="utc"

dt_imported=datetime.now() # utc
str_date_imported=dt_imported.strftime('%d%m%Y_%H%M')
print(str_date_imported)




# In[79]:


if is_py:
    press_Y=''
    ok=False

    if len(sys.argv) > 1:
        view_name=sys.argv[1]
    else:
        print("Enter the following input: ")
        view_name = input("View Table Name : ")
print(f"View name to load to BQ :{view_name} ")


# In[80]:


def get_key_id_by_view_name(view_name):

    if view_name == "pmr_pm_plan":
        key_name = "pm_id"
    elif view_name == "pmr_pm_item":
        key_name = "pm_item_id"
    elif view_name == "pmr_project":
        key_name = "project_id"
    elif view_name == "pmr_inventory":
        key_name = "inventory_id"     
    elif view_name == "xyz_incident":
        key_name = "incident_id"   
    else:
        raise Exception("No specified content type id")
        
    return key_name                       
key_id=get_key_id_by_view_name(view_name)
print(key_id)


# # Config DB and BQ

# In[81]:


env_path='.env'
config = dotenv_values(dotenv_path=env_path)
print(env_path)

bq_table_name=view_name.replace('pmr_','')
print(f"{view_name} vs {bq_table_name}")


projectId='smart-data-ml'
credentials = service_account.Credentials.from_service_account_file(r'C:\Windows\smart-data-ml-91b6f6204773.json')
client = bigquery.Client(credentials=credentials, project=projectId)
# client = bigquery.Client(project=projectId)
dw_dataset_id="SMartDataAnalytics"

dw_table_id = f"{projectId}.{dw_dataset_id}.{bq_table_name}"
print(dw_table_id)


# # Postgres &BigQuery

# In[82]:


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


# # Get data from View on Postgres DB

# In[83]:


def Get_ID_DB():
    sql_pg=f"""
    select {key_id} from {view_name} where updated_at AT time zone '{tz}' >= '{start_query}' 
    """
    print(sql_pg)
    df=list_data_pg(sql_pg,None,get_postgres_conn())
    return df
dfDB=Get_ID_DB()
dfDB.info()


# # Get data from Main table on BigQuery

# In[84]:


def Get_ID_BQ():
    sql_bq=f"""
    SELECT {key_id} FROM `{dw_table_id}` WHERE  is_deleted=False
    """
    print(sql_bq)
    df=load_data_bq(sql_bq)
    return df

dfBQ=Get_ID_BQ()
dfBQ.info()


# # Comparision

# In[85]:


def get_different_values(list1, list2):
  """Get the values list of the values in BQ that are not in DB vice versa.

  Args:
    list1: BQ or DB.
    list2: BQ or DB.

  Returns:
    A list of the values in BQ that are not in DB vice versa.
  """

  result = []
  for value in list1:
    if value not in list2:
      result.append(value)

  return result


    


# In[86]:


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

        print(f"Already deleted on {config['DATABASES_NAME']}: list values in BQ that are not in DB")
        # diffBQ=get_different_values(bqList,dbList)
        diffBQ=list(set(bqList)-set(dbList))
        print(diffBQ)
        return   diffDB,diffBQ

    else:
        print("Great")
        return [],[]
    

        
dbIDs,BQIds=find_diff_id(dfDB,dfBQ)  


# # Get data from SMARTDB that have been synchoize to BigQuery

# In[87]:


def get_comming_data(x_dbIDs,id):
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
        
        
result=get_comming_data(dbIDs,key_id)   
print(result)


# In[ ]:





# In[ ]:




