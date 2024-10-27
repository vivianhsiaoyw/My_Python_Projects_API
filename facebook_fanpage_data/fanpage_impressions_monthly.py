import os
import glob
import requests
import pandas as pd
import dask.dataframe as dd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
import gspread
import calendar


token_file = r'C:\Users\Vivian\Desktop\FB粉絲專頁\粉絲專頁token.txt'
page_id = 'fanpageid' #fill in your fb fanpage id
url = f'https://graph.facebook.com/{page_id}/insights/' #facebook api

# set month range
date_now = datetime.now().date()
rnge_std = -1  # start from last month
rnge_end = -1  

# save to google sheet
credential_dir=r'C:\Users\Vivian\Desktop\credentials.json'
sheet_key='googlesheetid'  # fill in your google sheet id

def connect_googlesheet():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    credentials = Credentials.from_service_account_file(credential_dir, scopes=scopes)
    gc = gspread.authorize(credentials)
    gs = gc.open_by_key(sheet_key)
    return gs

def connect_worksheet(year):
    gs = connect_googlesheet()

    try:
        worksheet = gs.worksheet(f'{year}_monthly')
    except:
        gs.add_worksheet(title=f'{year}_monthly', rows=1000, cols=20)
        worksheet = gs.worksheet(f'{year}_monthly')       
    return gs, worksheet

def get_token():
    with open(token_file, 'r') as f:
        data = f.read()
    return data

def fb_page_data(date):
    
    until_date = date + relativedelta(months=1)
    page_access_token = get_token()
    params = {
            'metric': 'page_impressions,page_impressions_unique',
            'access_token': page_access_token,
            'since': date.strftime('%Y-%m-01'), 
            'until': until_date.strftime('%Y-%m-01'),
            'period': 'total_over_range'
          }
    
    r = requests.get(url,params=params).json()
    
    blank = []
    try:
        for datas in r['data']:
            metric = datas['name'] #metric
            d = datas['values'] 
            df = pd.DataFrame.from_records(d)
            df['month'] = int(date.strftime('%Y%m'))
            df['year'] = int(date.strftime('%Y'))
            df[f'{metric}'] = df['value']
            df = df.drop(['end_time', 'value'], axis=1)
            blank.append(df)
    except:
        pass
    if len(blank) > 0:
        df = pd.concat(blank, axis=1)
        df = df.loc[:,~df.columns.duplicated()]
        return df

def to_googlesheet(dff, eachyear):  
    gs, worksheet = connect_worksheet(eachyear)
    
    df_new = dff
    df_old = pd.DataFrame(worksheet.get_all_records())
    
    if df_old.empty == True:
        df = df_new
    else:
        df = pd.concat([df_old, df_new], sort=False)
    df.drop_duplicates(subset=['month'], keep='last', inplace=True, ignore_index=True)
        
    worksheet.clear()
    set_with_dataframe(worksheet=worksheet, dataframe=df, include_index=False, include_column_header=True, resize=True)
    print('Update Successfully!')

def groupby_year(df): # save data to google sheets by year
    grouped = df.groupby(df.year)
    for year in df.year.unique():
        df = grouped.get_group(year)
        df = df.drop(['year'], axis=1)
        to_googlesheet(df, year) 
        print(f'finish concat {year}')

def main_monthly_loop():
    concat_df = []
    for delta in range(rnge_std, rnge_end - 1, -1):
        date_delta = date_now + relativedelta(months=delta)
        forfb_date = date_delta
        print(f'Start {forfb_date.strftime("%Y%m")}')
        df = fb_page_data(forfb_date)
        concat_df.append(df)
    df = pd.concat(concat_df, ignore_index=1)
    df = df.sort_values(by='month', ascending=True)
    groupby_year(df)
    
def main():
    print('Start')
    main_monthly_loop()

if __name__ == '__main__':
    main()