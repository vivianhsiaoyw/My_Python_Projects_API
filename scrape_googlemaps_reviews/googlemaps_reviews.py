"""
This script automates the retrieval of location details and names from your Google Maps account. 
It also fetches reviews and associated replies for each location, enabling streamlined analysis and management of customer feedback.

- Connects to the Google Maps API to retrieve business location information.
- Extracts customer reviews and their corresponding replies for better insights.
- Supports scalable data collection for multiple business locations.
- Provides a foundation for sentiment analysis, review trend tracking, or customer satisfaction evaluation.
"""


import pandas as pd
import dask.dataframe as dd
import requests
import os
import glob
import math
import json
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Set the file location and file name for saving
config_dir = r'C:\Users\Vivian\Desktop\config_data'
save_dir = r'C:\Users\Vivian\Desktop'
location_list_filename = 'locations.csv'
review_summ_filename = 'reviews_summ.csv'
review_detail_filename = 'reviews_detail.csv'

# Set up Google Maps credentials and token
refresh_token_file = r'\GoogleBusinessApi_refresh.txt'
access_token_file = r'\GoogleBusinessApi_access.txt'
account_details = './account.json'
gcp_client = './client_secrets.json'

# Set up Google Maps API url path
refresh_token_url = 'https://accounts.google.com/o/oauth2/token'
accounts_api = 'https://mybusinessaccountmanagement.googleapis.com/v1/accounts'
locations_api = 'https://mybusinessbusinessinformation.googleapis.com/v1/account/locations'
reviews_api = 'https://mybusiness.googleapis.com/v4/account/location/reviews'
locations_api_bat = 'https://mybusiness.googleapis.com/v4/account/locations:batchGetReviews'

def read_config(config_file: str):
    with open(config_file, 'r', encoding='utf-8') as file:
        data = json.load(file, strict = False)
        return data

def fuct_to_csv(df, fn):
    """Save file to excel"""
    df.to_csv(fn, sep='\t', encoding='utf_8_sig', date_format='string',
              index=False, chunksize=10**5)

def rsp_getnextpagecnt(report):
    """If there are multiple pages of data, retrieve the next page using the provided next page token."""
    try:
        nextpagecnt = report.get('nextPageToken')
        # print(f'skip {nextpagecnt}')
        return nextpagecnt
    except Exception as e:
        print(e)
        return ""

def sleep(i):
    """Set the sleep time to control the rate of requests sent to the API."""
    if i % 30 == 0:
        print('sleep for 10s')
        time.sleep(10)

# for read and refresh token if necessary
class Token:
    """
        The class consolidates all functions related to token management, including reading, refreshing API.
        ''refresh_token'' Refresh authentication tokens if necessary.
    """
    def __init__(self):
        self.config_gcp = read_config(gcp_client)
        self.config = self.config_gcp.get('web')
        self.clientid = self.config.get('client_id')
        self.clientsecret = self.config.get('client_secret')

    def read_refresh_token(self):
        token_file = open(config_dir + refresh_token_file, "r")
        token = token_file.read()
        return token
    
    def refresh_token(self):
        refresh_token = self.read_refresh_token()
        payload = {
            'client_id' : f'{self.clientid}',
            'client_secret' : f'{self.clientsecret}',
            'grant_type' : 'refresh_token',
            'redirect_uri' : 'https%3A%2F%2Fdevelopers.google.com%2Foauthplayground',
            'access_type' : 'offline',
            'approval_prompt' : 'force',
            'refresh_token' : f'{refresh_token}'
            }
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
    
        response = requests.request("POST", refresh_token_url, headers=headers, data=payload).text
        # print(response)
        refresh_token = json.loads(response).get('access_token')
        with open(config_dir + access_token_file, 'w') as f:
            f.write(refresh_token)
    
    def read_access_token(self):
        token_file = open(config_dir + access_token_file, "r")
        token = token_file.read()
        return token

class Locations:
    """
        The class is designed to retrieve and process location data from a Google account.
        
        - `locations_API()`: Fetches location data from the API by making a request using the account’s access token.
        - `trans_location_df()`: Transforms the API response into a structured DataFrame by extracting and formatting the address information. It also appends the account name and prepares the final DataFrame for use.
        - `get_locationsid()`: Calls the API to fetch all locations, handles pagination, and processes the data into a DataFrame, which is then transformed into a readable format using `trans_location_df()`.
    """
    def __init__(self, account):
        self.location_list = []
        self.account = account
    
    def locations_API(self, pagetoken):
        url = locations_api.replace('account', self.account)
        access_token = Token().read_access_token()
        payload = {
            'pageToken' : pagetoken,
            'pageSize' : 100,
            'orderBy' : 'storeCode desc',
            'read_mask' : 'name,title,storeCode,storefront_address'
            }
        headers = {
            'Authorization': f'Bearer {access_token}'
        }
        response = requests.request("GET", url, headers=headers, params=payload).text
        print(response)
        df = json.loads(response)
        return df
    
    def trans_location_df(self, df):
        df = pd.concat([df, df['storefrontAddress'].apply(pd.Series)], axis=1)
        df = pd.concat([df, pd.DataFrame(df['addressLines'].apply(pd.Series)).rename(columns=lambda x: "storeaddress"+str(x))], axis=1)
        df = df.fillna("")
        sel_col = [col for col in df.columns if 'storeaddress' in col]
        df['addressLine'] = df[sel_col].apply(lambda x: ' '.join(x), axis=1)
        df['address'] = df[['administrativeArea', 'locality', 'addressLine']].apply(lambda x: ''.join(x), axis=1)
        df['account'] = self.account
        rst_df = df[["account", "name", "storeCode", "title", "postalCode", "address"]]
        return rst_df
    
    def get_locationsid(self):
        data = self.locations_API(None)
        location_ids = data.get('locations')
        ids_df = pd.DataFrame.from_dict(location_ids)
        pagetoken = rsp_getnextpagecnt(data)
        self.location_list.append(ids_df)
        
        while pagetoken != None:
            resp = self.locations_API(pagetoken)
            location_ids = resp.get('locations')
            ids_df = pd.DataFrame.from_dict(location_ids)
            pagetoken = rsp_getnextpagecnt(resp)
            self.location_list.append(ids_df)
    
        rst_df = pd.DataFrame()
        try:
            if len(self.location_list) > 0:
                rst_df = pd.concat(self.location_list)
    
        except Exception as e:
            print(e)
        
        rst_df = self.trans_location_df(rst_df)
        return rst_df
        
    def locations_tocsv(self):
        rst_df = self.get_locationsid()
        fuct_to_csv(rst_df, location_list_filename)

    def read_locationsid():
        df = pd.read_csv(f'{location_list_filename}', sep='\t')
        return df
 
class Reviews_bat:
    """The class retrieves ratings and review data for a list of locations from the 'Locations' class."""
    
    def __init__(self, locations_list, account):
        self.location_id = locations_list
        self.account = account

    def reviews_bat_API(self, pagetoken):
        url = locations_api_bat.replace('account', self.account)
        
        access_token = Token().read_access_token()
        payload = {         
            "locationNames": 
              [self.location_id]
            ,
            "pageSize": 50,
            "pageToken": pagetoken
            }
        
        headers = {
            'Authorization': f'Bearer {access_token}'
        }
        response = requests.request("POST", url, headers=headers, data=payload).text
        print(response)
        df = json.loads(response)
        return df

class Reviews:
    """
    The class retrieves reviews detail, including reviewer names and comments for a specific location.
    
    - `reviews_API()`: Sends a request to the API to fetch reviews data for the location. Handles pagination using `pagetoken`.
    - `get_reviews_detailall()`: Processes detailed review data, including reviewer names, comments and ratings.
    - `get_reviews_summ()`: Extracts and processes the summary information from the reviews data.
    - `reviews_page_loop()`: Loops through all available pages of reviews and consolidates the details and summary into two DataFrames.
    - `refreshtoken_again()`: Attempts to refresh the access token and retries fetching reviews in case of an error.
    """
    def __init__(self, location_id):
        self.review_summ_list = []
        self.review_detail = []
        self.account = location_id['account']
        self.id = location_id['name']
        self.shopid = location_id['storeCode']

    def reviews_API(self, pagetoken):
        url = reviews_api.replace('account', self.account).replace('location', self.id)
        
        access_token = Token().read_access_token()
        payload = {
            'pageToken' : pagetoken,
            'pageSize' : 50
            }
        headers = {
            'Authorization': f'Bearer {access_token}'
        }
        response = requests.request("GET", url, headers=headers, params=payload).text
        # print(response)
        df = json.loads(response)
        return df
    
    def get_reviews_detailall(self, reviews):
        df = reviews.get('reviews')
        if df is None:
            df = []
        for r_list in df:
            sel_column = {x: r_list[x] for x in r_list if x not in {'reviewer', 'name', 'reviewReply'}}
            df = pd.DataFrame([sel_column])
            self.review_detail.append(df)
            rst_df = pd.DataFrame()
        rst_df = pd.concat(self.review_detail, ignore_index=1)
        rst_df = rst_df.replace(r'\n',' ', regex=True) 
        rst_df['storeCode'] = self.shopid
        return rst_df
        
    def get_reviews_summ(self, data):
        js = data
        sel_column = {x: js[x] for x in js if x not in {"reviews", "nextPageToken"}}
        for k, v in sel_column.items():
            pass
        df = pd.DataFrame(sel_column.items()).set_index(0).T
        df['storeCode'] = self.shopid
        return df
        
    def reviews_page_loop(self):
        print(f'start {self.shopid}')
        data = self.reviews_API(None)

        if len(data) > 0:
            reviews = self.get_reviews_detailall(data)
            reviews_summ_df = self.get_reviews_summ(data)
            pagetoken = rsp_getnextpagecnt(data)
            
            while pagetoken != None:
                resp = self.reviews_API(pagetoken)
                reviews = self.get_reviews_detailall(resp)
                pagetoken = rsp_getnextpagecnt(resp)
            
            return reviews, reviews_summ_df
        else:
            blank_df = pd.DataFrame()
            return blank_df, blank_df
        
    def refreshtoken_again(self):
        try:
            reviews, reviews_summ_df = self.reviews_page_loop()
            
        except:
            print("refresh token again!!!")
            Token().refresh_token()
            reviews, reviews_summ_df = self.reviews_page_loop()
            
        return reviews, reviews_summ_df

def loop_shops_reviews():
    """
        Use the 'Reviews' class to retrieve reviewer names, comments, and ratings for locations.
    """
    
    summ_list = []
    detail_list = []
    locations = Locations.read_locationsid()
    location_list = (locations['account']+locations['name']).tolist()
    # print(location_list)
    
    i = 0
    for index, loc_id in locations.iterrows():
        i += 1
        sleep(i)
        
        rev_obj = Reviews(loc_id)
        reviews_all, reviews_summ = rev_obj.refreshtoken_again()
        summ_list.append(reviews_summ)
        detail_list.append(reviews_all)
        summ_df = pd.concat(summ_list, ignore_index=1).drop_duplicates(subset=['storeCode'])
        detail_df = pd.concat(detail_list, ignore_index=1).drop_duplicates(subset=['reviewId'])
    os.chdir(save_dir)
    fuct_to_csv(summ_df, review_summ_filename)
    fuct_to_csv(detail_df, review_detail_filename)
    
def loop_shops_reviews2():
    """
        Use the 'Reviews_bat' class to retrieve retrieves ratings and review data for locations.
    """
    
    detail_list = []
    summ_list = []
    location_list = Locations.read_locationsid()

    location_list = [v for k, v in location_list.groupby('account')]
    for l in location_list:
        account = l['account'][0]
        location_list = l[['account', 'name']].apply(lambda x: '/'.join(x), axis=1)
        for item in location_list:
            detail_list.append(item)
            rev_obj = Reviews_bat(detail_list, account)
            rev_obj.reviews_bat_API(None)
            reviews_all = rev_obj.refreshtoken_again()
            summ_list.append(reviews_all)
            detail_df = pd.concat(summ_list, ignore_index=1)
        os.chdir(save_dir)
        fuct_to_csv(detail_df, review_detail_filename)

def loop_account():
    """
        Loops through multiple accounts and retrieves location data for each account.
    """
    
    location_list = []
    config_account = read_config(account_details)
    accounts = config_account.get("accounts")
    for account in accounts:
        account_name = account.get("name")
        df = Locations(account_name).get_locationsid()
        location_list.append(df)
        loc_df = pd.concat(location_list, ignore_index=1)
    fuct_to_csv(loc_df, location_list_filename)    

def main():
    
    # for refresh token (optional)
    # Token().refresh_token()
    
    # for update locations list (optional)
    # loop_account()
    # Locations.read_locationsid()

    # get reviews
    # loop_shops_reviews()
    loop_shops_reviews2()

if __name__ == '__main__':  
    print("start")
    main()
    print("end")
