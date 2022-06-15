#!/usr/bin/env python
# coding: utf-8

# In[ ]:





# In[22]:


import psycopg2
import pandas as pd
import numpy as np
import datetime
import db_dtypes
from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file('/home/airflow/gcs/data/googlekey.json')
client = bigquery.Client(credentials=credentials)



#establishing the connection
conn = psycopg2.connect(
   database="oltp", user='postgres', password='7798748934', host='34.66.167.79', port= '5432'
)
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
conn.autocommit = True


# In[23]:


df_consumer_master=pd.read_sql(f'''select * from customer_master''',conn)
df_product_master=pd.read_sql(f'''select * from product_master''',conn)
df_order_details=pd.read_sql(f'''select * from order_details''',conn)
df_order_items=pd.read_sql(f'''select * from order_items''',conn)


# In[ ]:





# In[24]:


# creating dim_order table using pandas resources
def dim_order(df_order_details):
    g = df_order_details
    g = df_order_details.groupby("orderid").tail(1)
    x = g[["orderid","order_status_update_timestamp","order_status"]]
    dim_order = x.reset_index()
    del dim_order["index"]
    return dim_order


# In[25]:


dim_order = dim_order(df_order_details)


# In[26]:


# creatin fact_daily_orders

def fact_daily_orders(df_consumer_master,df_order_details,df_order_items):
    x=df_order_details.groupby("orderid").head(1)[["customerid","orderid","order_status_update_timestamp"]].reset_index()
    del x["index"]
    y=df_order_details.groupby("orderid").tail(1)["order_status_update_timestamp"].reset_index()
    del y["index"]     
    fact_daily_orders = pd.concat([x,y],axis=1)
    fact_daily_orders.columns = ["customerid","orderid","order_received_timestamp","order_delivery_timestamp"]
    l=[]
    for i in fact_daily_orders["customerid"]:
             l.append(int(df_consumer_master.where(df_consumer_master["customerid"]==i).dropna()["pincode"]))
            # l is pincode column
    fact_daily_orders["pincode"] = l
        
    
    k=[]
    for i in df_order_items["productid"]:
        k.append(int(df_product_master.where(df_product_master["productid"]==i).dropna()["rate"] * df_order_items.iloc[i,2] ))
    df_order_items["Total"]=k    
    k=[]
    k = df_order_items.groupby('orderid').sum()["Total"]
    k1 = df_order_items.groupby('orderid').sum()["quantity"]
    fact_daily_orders["order_amount"]=list(k)
    fact_daily_orders["item_count"]=list(k1)
    fact_daily_orders["order_delivery_time_seconds"] =fact_daily_orders["order_delivery_timestamp"] - fact_daily_orders["order_received_timestamp"]
    return fact_daily_orders


# In[27]:


fact_daily_orders = fact_daily_orders(df_consumer_master,df_order_details,df_order_items)


# In[28]:


# creating f_order_details
def f_order_details(df_order_details,df_order_items):
    f_order_details = pd.merge(df_order_details.groupby("orderid").tail(1), df_order_items, how='inner')[["orderid","order_status_update_timestamp","productid","quantity"]]
    f_order_details.columns = ["orderid","order_delivery_timestamp","productid","quantity"]
    return f_order_details


# In[29]:


f_order_details = f_order_details(df_order_details,df_order_items)


# In[30]:


# creating dim_customer
def dim_customer(df_consumer_master):
    df_consumer_master["address_id"] = list(range(1,1001))
    dim_customer =df_consumer_master[["customerid","name","address_id"]]
    dim_customer["start_date"]= list(df_consumer_master['update_timestamp'].dt.date)
    dim_customer["end_date"] = np.nan
    return dim_customer


# In[31]:


dim_customer = dim_customer(df_consumer_master)


# In[41]:


# creating dim_address
def dim_address(df_consumer_master):
    df_consumer_master["addressid"] = list(range(1,1001))
    dim_address = df_consumer_master[["addressid","address","city","state","pincode"]]
    return dim_address


# In[42]:


dim_address = dim_address(df_consumer_master)


# In[34]:


# creationg dim_product
def dim_product(df_product_master):
    l=[]
    k=[]
    dim_product = df_product_master
    dim_product["start_date"]=np.nan
    dim_product["end_date"]=np.nan
    return dim_product
    


# In[35]:


dim_product=dim_product(df_product_master)


# In[36]:


#connection with bigquery


# In[62]:


tableRef1 = client.dataset("starschema").table("dim_address")
client.load_table_from_dataframe(dim_address,tableRef1)


# In[44]:


tableRef2 = client.dataset("starschema").table("dim_order")
client.load_table_from_dataframe(dim_order,tableRef2)


# In[60]:


tableRef3 = client.dataset("starschema").table("fact_daily_orders")
client.load_table_from_dataframe(fact_daily_orders,tableRef3)


# In[63]:


tableRef4 = client.dataset("starschema").table("dim_customer")
client.load_table_from_dataframe(dim_customer,tableRef4)


# In[64]:


tableRef5 = client.dataset("starschema").table("dim_product")
client.load_table_from_dataframe(dim_product,tableRef5)


# In[65]:


tableRef6 = client.dataset("starschema").table("f_order_details")
client.load_table_from_dataframe(f_order_details,tableRef6)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




