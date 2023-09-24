# %reload_ext autoreload
# %autoreload 2
# price fetcher using different sources

import os,sys
from dotenv import load_dotenv
from web3 import Web3 
import numpy as np
import pandas as pd
import json
from decimal import Decimal
import datetime
import hashlib
import requests
import logging
import hexbytes




logger = logging.getLogger(__name__)
        

#gloabal variables
now = datetime.datetime.now()
# Format the date to 'day-month-year'
DATE = now.strftime('%d%m%y')

class Price_generic():
    def __init__(self, eth_tracker_loc, w3, API):
        self.w3 = w3
        self.apis= API
        self.ET_root = eth_tracker_loc

    def check_dir(self, path):
        if(not(os.path.exists(path))):
            os.makedirs(path)

    def unixtime_to_blocknum(self, unix_time):
        # binary search
        low, high = 0, self.w3.eth.get_block_number()

        while low <= high:
            mid = int((low + high) / 2)
            mid_block_timestamp = self.w3.eth.get_block(mid).timestamp

            if mid_block_timestamp == unix_time:
                return mid
            elif mid_block_timestamp < unix_time:
                low = mid + 1
            else:
                high = mid - 1

        return high  # Return the closest preceding block number


    def blocknum_to_unixtime(self, blocknum):
        block = self.w3.eth.get_block(blocknum)
        return block.timestamp


    def unixtime_to_datetime(self, unix_timestamp):
        dt = datetime.datetime.utcfromtimestamp(unix_timestamp)
        return dt.strftime('%d%m%y')
    
    def datetime_to_unixtime(self, date_string):
        dt = datetime.datetime.strptime(date_string, '%d%m%y')
        return int(dt.timestamp())


    def datetime_to_blocknum(self, date_string):
        ts = self.datetime_to_unixtime(date_string)
        bn = self.unixtime_to_blocknum(ts)
        return bn
    
    def convert_to_decimal(self, obj):
        if isinstance(obj, dict):
            # If the object is a dictionary, convert its values
            return {key: self.convert_to_decimal(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            # If the object is a list, convert its elements
            return [self.convert_to_decimal(element) for element in obj]
        elif isinstance(obj, str):
            # If the object is a string, try to convert it to a Decimal
            try:
                return Decimal(obj)
            except:
                return obj
        else:
            # If the object is anything else, return it unchanged
            return obj
        
####################################################################################################

class Defillama(Price_generic):
    def __init__(self,eth_tracker_loc, w3, API):
        super().__init__(eth_tracker_loc, w3, API)
       
    def parse_token_addrs(self, args):

        if('csv' in args.token[0]):
            token_addrs = pd.read_csv(args.token[0])['tokens'].tolist() 
        else:
            token_addrs = args.token
        
        token_aliases = []
        for token_addr in token_addrs:
            if(token_addr == 'ETH'):
                token_aliases.append('coingecko:ethereum')
            else:
                token_aliases.append('ethereum:'+token_addr)

        return token_aliases
    
    def parse_token_addrs_batch(self, args, batch_size = 100):

        if('csv' in args.token[0]):
            token_addrs = pd.read_csv(args.token[0])['tokens'].tolist() 
        else:
            token_addrs = args.token
        
        token_aliases = []
        for token_addr in token_addrs:
            if(token_addr == 'ETH'):
                token_aliases.append('coingecko:ethereum')
            else:
                token_aliases.append('ethereum:'+token_addr)

        # make them a comma-separated long string 
        n_chunks = int(np.ceil(len(token_aliases)/batch_size))
        
        req_batch_list = []
        pointer = 0
        for ii in range(n_chunks):
            if(ii==n_chunks-1):
                curr_batch = token_aliases[pointer:]
            else:
                curr_batch = token_aliases[pointer:pointer+batch_size]
                pointer += batch_size
            req_batch_list.append(','.join(curr_batch))
            

        return req_batch_list

    def get_current_price(self, args):
        
        token_aliases = self.parse_token_addrs(args)
        
        result_df = pd.DataFrame()
        for token_alias in token_aliases:
            url = f"https://coins.llama.fi/prices/current/{token_alias}"
            params = {'searchWidth': '6h'} # default param of defillama
            headers = {'accept': 'application/json'}

            res = requests.get(url, params=params, headers=headers)

            if res.status_code == 200:
                print("Success:", res.json())
                res = res.json()
                formatted_row = self.format_response(res, token_alias, args)
                logger.info(f'formatting the fetched price response for {token_alias}')
                if(formatted_row is not None):
                    result_df = pd.concat([result_df, formatted_row])
                    
            else:
                logger.error(f"Failed: status code {res.status_code}")

        # reset index 
        result_df = result_df.reset_index(drop=True)

        # save 
        logger.info(f'saving the most recent price(s) for {args.token}')
        cache_root = f'./output/{DATE}/{args.source}/{args.job}'
        self.check_dir(cache_root)
        cache_file = f'{cache_root}/price.csv'            
        result_df.to_csv(cache_file, index=False)

    
    def get_current_price_batch(self, args):
        
        token_aliases = self.parse_token_addrs_batch(args)
        
        result_df = pd.DataFrame()
        for token_alias in token_aliases:
            url = f"https://coins.llama.fi/prices/current/{token_alias}"
            params = {'searchWidth': '6h'} # default param of defillama
            headers = {'accept': 'application/json'}

            res = requests.get(url, params=params, headers=headers)

            if res.status_code == 200:
                print("Success:", res.json())
                res_json = res.json()
                formatted_row = self.format_response(res_json, token_alias, args)
                logger.info(f'formatted the fetched price response for {token_alias}')
                if(formatted_row is not None):
                    result_df = pd.concat([result_df, formatted_row])
                    
            else:
                logger.error(f"Failed: status code {res.status_code}")

        # reset index 
        result_df = result_df.reset_index(drop=True)

        # save 
        logger.info(f'saving the most recent price(s) for {args.token}')
        cache_root = f'./output/{DATE}/{args.source}/{args.job}'
        self.check_dir(cache_root)
        cache_file = f'{cache_root}/price.csv'            
        result_df.to_csv(cache_file, index=False)


    def format_response(self, res_json, token_alias, args):
        res_batch = res_json['coins']        
        formatted_df = pd.DataFrame()
        for k, v in res_batch.items():
            if(len(v)>0):
                v['source_alias'] = k
                row_df = pd.DataFrame(v, index=[0])
                formatted_df = pd.concat([formatted_df, row_df])
            
            else:
                logger.error(f'no available price data from {args.source} for {token_alias}')


        formatted_df = formatted_df.reset_index(drop=True)

        return formatted_df


    # wip
    def get_price_historical(self):
        pass



# wip
class CoinGecko(Price_generic):
    def __init__(self,eth_tracker_loc, w3, API):
        super().__init__(eth_tracker_loc, w3, API)
       

    def get_current_price(self):
        pass


    def get_price_historical(self):
        pass


# other on-chain price fetchers might come later



    