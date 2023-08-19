import os,sys
import json
from decimal import Decimal
import datetime


def convert_to_decimal(obj):
    if isinstance(obj, dict):
        # If the object is a dictionary, convert its values
        return {key: convert_to_decimal(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        # If the object is a list, convert its elements
        return [convert_to_decimal(element) for element in obj]
    elif isinstance(obj, str):
        # If the object is a string, try to convert it to a Decimal
        try:
            return Decimal(obj)
        except:
            return obj
    else:
        # If the object is anything else, return it unchanged
        return obj


def check_dir(path):
    if(not(os.path.exists(path))):
        os.makedirs(path)


### handling times

class time_handler():
    def __init__(self, w3):
        self.w3 = w3
        assert self.w3.is_connected(), 'please check rpc provider configuration, web3 connection is not established'


    
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
    
    


