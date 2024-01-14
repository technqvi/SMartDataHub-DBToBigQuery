#!/usr/bin/env python
# coding: utf-8

# In[7]:


import os
import sys 
import CheckDataCons_DB_BQ as check_data


# In[ ]:





# In[8]:


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


result=check_data.check_data_consistency_db_bq(view_name)
if result:
    print("if result=True , view csv file in check_db_bq  data_consistence_check")


# In[ ]:




