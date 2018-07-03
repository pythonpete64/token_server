
# coding: utf-8

# token_server
# 
# every 30 seconds
# [x]    1. get a responce from the cmc api [resp]
# [ ]    2. check responce is > than last db update
# [x]    3. process responce(resp)
# [x]    4. ensert respnce into database
# 
#             
#     
# 

# In[1]:


import requests
import pandas as pd
import json
import datetime as dt
import json
import time
import os


# In[2]:



def convert_unix_time(unix_time):
    split = (dt.datetime.fromtimestamp(int(unix_time)).strftime('%Y-%m-%d %H:%M:%S')).split(" ")
    d = split[0].split("-")
    t = split[1].split(":")
    return dt.datetime(int(d[0]), int(d[1]), int(d[2]), int(t[0]), int(t[1]), 0)


# In[7]:



def create_symbol_list(json):

    symbol_list = []

    for k , v in json['data'].items():
        symbol_list.append(v['symbol'])
        
    return symbol_list


# In[35]:


def process_responce(resp, tdf):
 
    
    time = convert_unix_time(resp['data']['1']['last_updated'])
    time_range = pd.date_range(time, periods=1, freq='T')
        
    #the final dataframes to insert into the database
    df_price = tdf.copy()
    df_mcap = tdf.copy()
    
    df_price.index = time_range
    df_mcap.index = time_range

    for key, value in resp['data'].items():

        symbol = value['symbol']
        mcap = value['quotes']['USD']['market_cap']
        price = value['quotes']['USD']['price']
              
        #df_price = df_price.join(pd.DataFrame(price, index=time_range, columns=[symbol]))
        #df_mcap = df_mcap.join(pd.DataFrame(mcap, index=time_range, columns=[symbol]))
            
        df_price[symbol] = price
        df_mcap[symbol] = mcap

    return [df_price, df_mcap]


# In[41]:


def input_to_csv(dfs):
    # test if file is empty
    if os.stat("C:\\Users\\20115619\\token_server\\data\\price.csv").st_size == 0:
        dfs[0].to_csv('C:\\Users\\20115619\\token_server\\data\\price.csv')
        
    if os.stat("C:\\Users\\20115619\\token_server\\data\\mcap.csv").st_size == 0:
        dfs[1].to_csv('C:\\Users\\20115619\\token_server\\data\\mcap.csv')
    
    dfs[0].to_csv('C:\\Users\\20115619\\token_server\\data\\price.csv', header=False)
    dfs[1].to_csv('C:\\Users\\20115619\\token_server\\data\\mcap.csv', header=False)


# In[42]:


def setup():
    
    temp_resp = requests.get('https://api.coinmarketcap.com/v2/ticker/?limit=100').json()
    symbol_list = create_symbol_list(temp_resp)

    time = convert_unix_time(temp_resp['metadata']['timestamp'])
    time_range = pd.date_range(time, periods=1, freq='T')

    return pd.DataFrame(index=time_range, columns=symbol_list)


# In[43]:


def run():

    tdf = setup() 
    
    while True:
        price_cap = process_responce(requests.get('https://api.coinmarketcap.com/v2/ticker/?limit=100').json(), tdf)
        input_to_csv(price_cap)
        print(price_cap)
        time.sleep(5)
        

      
        
        
        


# In[ ]:


df_temp = setup()

run()

