#!/usr/bin/env python
# coding: utf-8

# In[7]:


import os
import sys 
import CheckDataCons_DB_BQ as check_data
import sqlite3
import pandas as pd
import numpy as np

view_name='pmr_pm_plan'
is_py=True
if is_py:
    press_Y=''
    ok=False

    if len(sys.argv) > 1:
        view_name=sys.argv[1]
    else:
        print("Enter the following input: ")
        view_name = input("View Table Name : ")
print(f"View name to load to BQ :{view_name} ")


# In[9]:
sqlite3.register_adapter(np.int64, lambda val: int(val))
sqlite3.register_adapter(np.int32, lambda val: int(val))
data_base_file="etl_web_admin/bq_cdc_etl_transaction.db"

def list_data_sqlite(sql):
    try:
        conn = sqlite3.connect(os.path.abspath(data_base_file))
        print(sql)
        df_item=pd.read_sql_query(sql, conn)
    except Exception as e:
        print("Failed to insert etl_transaction table", str(e))
    finally:
        if conn:
            conn.close()
    return df_item
def get_view_source(name):
    try:
        sql=f"select * from view_source where name='{name}' limit 1"
        dfView=list_data_sqlite(sql)
        if dfView.empty==False:
           view_source=dfView.iloc[0,:]
        else:
            error=f"Not found {view_name} view"
            raise Exception(error)
    except Exception as e:
        print("Failed to insert etl_transaction table", str(e))
        raise Exception(e)
        
    return view_source
view_source_sr= get_view_source(view_name)
print(view_source_sr)


result=check_data.check_data_consistency_db_bq(view_source_sr)
if result:
    print("if result=True , view csv file in check_db_bq  data_consistence_check")


# In[ ]:




