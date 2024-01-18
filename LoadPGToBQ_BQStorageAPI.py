#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import math
import os
import json
from datetime import datetime ,timezone

import smart_bq_storage_api.viewdb_to_bq  as x
import smart_bq_storage_api.incident_data_pb2 as pb2_incident

from google.protobuf.timestamp_pb2 import Timestamp
# # Parameter Argument

# In[2]:




def db_to_bq_by_bq_storage_api(**kwargs):





    csv_file = kwargs.get('csv_file', "incident_bq-storage-api.csv")
    view_name = kwargs.get('view_name', "xyz_incident")
    view_name_id = kwargs.get('view_name_id', "incident_id")

    datetimeCols = kwargs.get('datetimeCols', ["open_datetime","close_datetime"])
    pk_fkCols= kwargs.get('pk_fkCols', ['inventory_id', 'incident_id'])

    projectId = kwargs.get('projectId', "pongthorn")
    main_dataset_id = kwargs.get('main_dataset_id', 'SMartDataAnalytics')
    table_name=kwargs.get('table_name', "incident")

    dt_imported = kwargs.get('dt_imported',datetime.strptime( 
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),"%Y-%m-%d %H:%M:%S"))





    # # Init Const and Variable 

    # In[3]:


    upsert_json_file="incident_upsert.json"
    delete_json_file="incident_delete.json"


    # # BQ-Storage-API Data Transformation

    # In[ ]:


    print("Read Csv file to process")
    df=pd.read_csv(csv_file)
    print(df.info())


    # In[ ]:


    print("Add timestamp import")
    dtimestamp = Timestamp()
    dtimestamp.FromDatetime(dt_imported)
    update_at_micro_timestampe =dtimestamp.ToMicroseconds()
    df['update_at']=update_at_micro_timestampe 


    # In[ ]:


    print("Change action type")

    def change_action_merge_to_bq_storage_api(x):
        if x=="added" or x=="changed":
            return  "UPSERT"
        else:
            return "DELETE"   
    df["_CHANGE_TYPE"]=df['action'].apply(change_action_merge_to_bq_storage_api)
    df=df.drop(columns=['action'])


    # In[ ]:


    print("Make sure all pk and fk are Int64")
    if len(pk_fkCols)>0:
        df[pk_fkCols] = df[pk_fkCols].astype('Int64')


    # In[ ]:


    print(df.info())
    print(df.tail())


    # # Spit data into UPSERT and DELETE 

    # In[ ]:


    print("filter data for UPSERT dataframe")
    dfUpsert=df.query("_CHANGE_TYPE=='UPSERT'")
    print(dfUpsert.info())
    print(dfUpsert.tail())


    # In[ ]:


    print("filter data for DELETE dataframe")
    dfDelete=df.query("_CHANGE_TYPE=='DELETE'")
    print(dfDelete.info())
    print(dfDelete)


    # # Upsert dataframe tranformation

    # # Timezone and UTC Convert
    # ## If you convert any time of any tz to timestampe for converting to Microseconds , it wll turn into UTC  so DateTime is UTC
    # * to_char((abc.incident_datetime AT TIME ZONE 'Asia/Bangkok'::text),
    #            'YYYY-MM-DD HH24:MI'::text)   AS open_datetime
    # * to_char((abc.incident_datetime AT TIME ZONE 'UTC'::text),
    #                'YYYY-MM-DD HH24:MI'::text)   AS open_datetime
    # * https://www.epochconverter.com/
    # 
    # 
    # 

    # ## WorkAround Sol: null dattime is replaced with 0(GMT:1-1-1970 12:00:00 AM)

    # In[ ]:


    print("Upsert dataframe tranformation")
    if dfUpsert.empty==False:
        print("Convert strng to datetime and microseconds accordingly")

        def convert_string_to_datetime_timestamp_microseconds (dt_str):
            if isinstance(dt_str, str):
                dt=datetime.strptime(dt_str,"%Y-%m-%d %H:%M")

                x_timestamp = Timestamp()
                x_timestamp.FromDatetime(dt)
                micro_x =x_timestamp.ToMicroseconds()

                return micro_x
            else:
                return np.nan
        #        

        if len(datetimeCols)>0:
            for d in datetimeCols:
                #Check whick column contain null value if so, convert float64 to int 32"
                dfUpsert[d]=dfUpsert[d].apply(convert_string_to_datetime_timestamp_microseconds)
                dfUpsert[d]=dfUpsert[d].astype('Int64')

    # #         # handle null datetime value(workaround) for 2.0 but it seem to support in 3.0
            # 1970-01-01 00:00:00 UTC
            dfUpsert[d] = dfUpsert[d].fillna(0)

    dfUpsert.info()
    print(dfUpsert)


    # # Delete dataframe tranformation

    # In[ ]:


    print("Delete dataframe tranformation")
    dfDelete=dfDelete[[view_name_id,"_CHANGE_TYPE"]]
    dfDelete.info()
    print(dfDelete)


    # # Write Json File to ingest to BQ by Buffer Protocol

    # In[ ]:


    print("Write Json File (upsert and delete)  to ingest to BQ by Buffer Protocol")
    if  dfUpsert.empty==False:

        print(upsert_json_file)
        json_file_path=os.path.join(upsert_json_file)

        json_incident_data = json.loads(dfUpsert.to_json(orient = 'records'))
        with open(upsert_json_file, "w") as outfile:
            json.dump(json_incident_data, outfile)


    # In[ ]:


    if  dfDelete.empty==False:

        print(delete_json_file)
        json_file_path=os.path.join(delete_json_file)
        json_incident_data = json.loads(dfDelete.to_json(orient = 'records'))
        with open(json_file_path, "w") as outfile:
            json.dump(json_incident_data, outfile)


    # # BufferProto to BQ 

    # In[ ]:


    print("BufferProto to BQ")


    # In[ ]:


    def get_data_pb2(view_name):

        x_data_pb2=None
        if view_name == "xyz_incident": 
            x_data_pb2=pb2_incident.IncidentData()
        else:
            raise Exception("No specified view name to get data pb2")

        return x_data_pb2


    # In[ ]:


    listColumns= df.columns.tolist()
    print(listColumns)


    # In[ ]:


    pb2_data=get_data_pb2(view_name)
    print(pb2_data.DESCRIPTOR)


    # # Ingest data to Bigquery

    # In[ ]:


    print(" Ingest data to Bigquery")


    # In[ ]:


    if os.path.exists(upsert_json_file):
        print("OK-Upsert")
        x.write_json_to_bq(listColumns=listColumns,
                                  x_data_pb2=pb2_data,
                                  json_data_file=upsert_json_file,
                                  project_name=projectId,
                                  dataset_name=main_dataset_id,
                                  table_name=table_name)




    # In[ ]:


    if os.path.exists(delete_json_file):
        print("OK-Delete")
        x.write_json_to_bq(listColumns=listColumns,
                                  x_data_pb2=pb2_data,
                                  json_data_file=delete_json_file,
                                  project_name=projectId,
                                  dataset_name=main_dataset_id,
                                  table_name=table_name)



    # # Delete all processed files

    # In[ ]:


    print("Delete all files") 
    listFilesToBeDeleted=[csv_file,upsert_json_file,delete_json_file]
    for file in listFilesToBeDeleted:
        if os.path.exists(file):
            print(file)
            os.remove(file)


    # In[ ]:


    return True


    # In[ ]:





    # In[ ]:




