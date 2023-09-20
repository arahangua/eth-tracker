import os,sys
from dotenv import load_dotenv
from web3 import Web3 
import numpy as np
import pandas as pd
import json
from decimal import Decimal
import datetime
import hashlib
import eth_abi 
import requests
import logging
import hexbytes


logger = logging.getLogger(__name__)
        

#gloabal variables
now = datetime.datetime.now()
# Format the date to 'day-month-year'
DATE = now.strftime('%d%m%y')
RETRY_UNVERIFIED=False


### util class for controlling eth-tracker class. handles time conversions and simple path handling.

class Transfer_Decoder():
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
# below are decoding related functions 

    def decode_input(self, input, contract_addr, contract_abi):
        contract = self.w3.eth.contract(address=contract_addr, abi=contract_abi)

        # check if it is a proxy contract 
        impl_check = 'implementation' in contract_abi.lower()
        upgrade_check = 'upgrade' in contract_abi.lower()
        verdict = impl_check and upgrade_check
        if(verdict):
            logger.info(f'found a proxy contract ({contract_addr}). Fetching the implementation function to get the matching contract (for now only openzeppelin upgradable proxy is handled.)')
            # somehow nowadays proxy contracts prevent clients from calling implementation function directly.
            #implementation_address = contract.functions.implementation().call()

            # for openzeppelin upgradable template
            padded = Web3.toHex(self.w3.eth.get_storage_at(contract_addr, "0x7050c9e0f4ca769c69bd3a8ef740bc37934f8e2c036e5a723fd8ee048ed3f8c3"))
            padded = padded[-42:]
            impl_addr = '0x' + padded[2:]
            if(impl_addr=='0x0000000000000000000000000000000000000000'):
                logger.info(f'found a proxy but {contract_addr} is not using openzeppelin upgradable contract... moving on')
                func = input[:10]
                params = 'unknown_proxy' 
                return func, params

            else:
                logger.info(f'found a proxy contract that probably is using openzeppelin upgradable contract')
                impl_addr = Web3.to_checksum_address(impl_addr)
                contract_abi, verdict = self.get_contract_abi(impl_addr, ETHERSCAN_API=self.apis['ETHERSCAN_API'])
                contract = self.w3.eth.contract(address=contract_addr, abi=contract_abi)
        
        func, params = contract.decode_function_input(input)
        
        return func, params
    
    # main function that handles different transfer functions
    def decode_trace_csv(self, csv_file, search_str='transfer'):
        csv_df = pd.read_csv(csv_file)

        # first get unique transaction positions logged in this csv. 

        tx_positions = pd.unique(csv_df['transactionPosition'])
        
        # this concat df includes multiple tx positions in a chronological order.
        concat = pd.DataFrame() 
        for tx_pos in tx_positions:
            logger.info(f'decoding csv file : {csv_file}, transaction position : {tx_pos}')
            tx_df = csv_df[csv_df['transactionPosition']==tx_pos]

            # get the first trace 
            init_row = tx_df[tx_df['traceAddress']=='[]']

            # initiating address
            
            init_addr = init_row['from']
            init_contract = init_row['to']

            # sort traces in a chronological order if necessary, *already sorted in a chronological order (miner set sequence) atm.
            
            if(search_str=='transfer'):
                targets = tx_df[tx_df['decoded'].str.contains(search_str + '|unwrapping')]
            
            else:
                targets = tx_df[tx_df['decoded'].str.contains(search_str)]
            # rest = self.sort_trace_rows(rest) 

            # decode each row
            
            for ii, row in targets.iterrows():
                decoded = {}
                decoded['init_address'] = init_addr
                decoded['init_contract'] = init_contract
                decoded['blockNumber'] = row['blockNumber']
                decoded['tx_pos'] = row['transactionPosition']
                
                row_decoded = self.trace_decoding_handler(row)
                if(row_decoded is not None):
                    for k, v in row_decoded.items():
                        decoded[k] = v
                else:
                    logger.error(f'continuing to the next trace as decoding was unsuccessful')
                    continue

                decode_df = pd.DataFrame(decoded, index=[0])
                concat = pd.concat([concat, decode_df])
        
        
        concat = concat.reset_index(drop='index')
        # remove duplicate traces from proxy calls
        concat = concat[concat['decimal']!=0]

        return concat
    
    # need to sure that abi is there
    def prep_contract(self, contract_addr):
        contract_addr = Web3.to_checksum_address(contract_addr)
        
        contract_abi, verdict = self.get_contract_abi(contract_addr, ETHERSCAN_API=self.apis['ETHERSCAN_API'])
        # check if it is a proxy contract 
        impl_check = 'implementation' in contract_abi.lower()
        upgrade_check = 'upgrade' in contract_abi.lower()
        verdict = impl_check and upgrade_check
        if(verdict):
            # for openzeppelin upgradable template
            padded = Web3.toHex(self.w3.eth.get_storage_at(contract_addr, "0x7050c9e0f4ca769c69bd3a8ef740bc37934f8e2c036e5a723fd8ee048ed3f8c3"))
            padded = padded[-42:]
            impl_addr = '0x' + padded[2:]
            impl_addr = Web3.to_checksum_address(impl_addr)
            contract_abi, verdict = self.get_contract_abi(impl_addr, ETHERSCAN_API=self.apis['ETHERSCAN_API'])
            contract = self.w3.eth.contract(address=contract_addr, abi=contract_abi)
            return contract
   
        # just return the usual contract variable if the contract is not a proxy contract        
        contract = self.w3.eth.contract(address=contract_addr, abi=contract_abi)

        return contract
    
    # not needed for now.
    def sort_trace_rows(self, rest:pd.DataFrame):
        pass


    
        
    def trace_decoding_handler(self, one_trace):
        contract_addr = one_trace['to']
        hex_input = one_trace['input']
        trace_value = one_trace['value']
        
        
        contract_addr = Web3.to_checksum_address(contract_addr) 

        # first check if ABI exists on Etherscan
        contract_abi, verdict = self.abi_handler_addr_pos(contract_addr, self.apis['ETHERSCAN_API'])
       
        # try to decode the input using ABI
        params = 'unused' # will be overwritten if ABI decoding was successful
        if(verdict=='contract'):
            if(len(hex_input)>2): #handling null and 0x
                if(contract_abi is None):
                    logger.info(f'fetching ABI failed. Trying to query public byte library')
                    # get first 8 hex digits (4 bytes) + 2 (0x)
                    hex_signature = hex_input[:10] 
                    text_signature = self.public_library_check(hex_input)
                    if(text_signature is None):
                        decoded = hex_signature
                    else: 
                        decoded = text_signature
                    
                else: # in case we have a matching ABI
                    try:
                        func, params = self.decode_input(hex_input, contract_addr, contract_abi)
                    except Exception as e:
                        print(e)
                        logger.error(f'suspecting a client problem (for decoding inputs using ABI). If the error was about \"insufficientDataBytes\" it could be a geth problem. https://github.com/ethereum/web3.py/issues/1257')
                        decoded = self.public_library_check(hex_input)
                        
                    # return decoded input
                    if(params=='unknown_proxy'):
                        decoded = func
                    elif(params=='ABI_reading_problem'):
                        decoded = func
                    else:
                        decoded = func.function_identifier
            # if input is 0x, need to check a value. If value is also 0x then likely a fallback function and if value is not 0x then likely a unwrapping (ether transfer) is happening.
            else:
                if(len(trace_value)>2):
                    eth_convert = int(trace_value, 16)/10**18
                    logger.info(f'likely a unwrapping event (ether transfer) {eth_convert:.3f} ETH')
                    decoded = 'unwrapping'
                   
                else:
                    logger.info(f'probably a fallback function of a contract(\'to\') getting called')
                    decoded = 'fallback'
        else:
            # for cases where it was an ether transfer or a contract creation

            # logger.info(f'\'to\' address was not a contract')
            if(len(hex_input)>2):
                logger.info(f'possible contract creation')
                decoded = 'contract creation'
            else:
                logger.info(f'simple ether transfer')
                decoded = 'ether transfer'

        # prepare decoded variable from the given the above case handling 
        decoded_params = self.decode_input_from_signature(decoded, params, one_trace)
        
        
        return  decoded_params
    

    # not used for now.
    def parse_dtype_from_sig(self, text_signature):
        pass

    def get_erc20_denom(self, contract_address):
        contract = self.prep_contract(contract_address)
        
        # get symbol 
        symbol = contract.functions.symbol().call()

        # get denominator
        decimal = contract.functions.decimals().call()

        # if(contract_address.lower()=='0xa2327a938febf5fec13bacfb16ae10ecbc4cbdcf'):
        #     symbol = 'USDC'
        #     decimal = 6

        return symbol, decimal

    # all ether transfers and standard erc20 transfers will be handled.
    def decode_input_from_signature(self, decoded, params, one_trace):
        
        # erc20 transfer
        if(decoded == 'transfer'):
            decoded_params = self.transfer_handler(decoded, params, one_trace)
        
        elif(decoded == 'transferFrom'):
            
            decoded_params = self.transferFrom_handler(decoded, params, one_trace)
          
        elif(decoded =='ether transfer'):
            decoded_params= {}
            decoded_params['from']= one_trace['from']
            decoded_params['to'] = one_trace['to']
            decoded_params['token_addr']= 'ETH'
            decoded_params['symbol'] = 'ETH'
            decoded_params['decimal'] = 18
            decoded_params['value']=   int(one_trace['value'], 16)/10**18
            decoded_params['function'] = decoded

        elif(decoded=='unwrapping'):
            decoded_params= {}
            decoded_params['from']= one_trace['from']
            decoded_params['to'] = one_trace['to']
            decoded_params['token_addr']= 'ETH'
            decoded_params['symbol'] = 'ETH'
            decoded_params['decimal'] = 18
            decoded_params['value']=   int(one_trace['value'], 16)/10**18
            decoded_params['function'] = decoded

      
        else:
            logger.error(f'found unhandled function (or signature): {decoded}')
            decoded_params= None
            

        return decoded_params

    def transfer_handler(self, decoded, params, one_trace):
        decoded_params= {}
        
        if('recipient' in params):
            decoded_params['to'] = params['recipient']
        elif('_recipient' in params):
            decoded_params['to'] = params['_recipient']
        
        # src, dst, wad case
        elif('dst' in params):
            decoded_params['to'] = params['dst']
        elif('_dst' in params):
            decoded_params['to'] = params['_dst']

        elif('to' in params):
            decoded_params['to'] = params['to']
        
        elif('_to' in params):
            decoded_params['to'] = params['_to']
        else:
            raise ValueError(f'new syntax for transfer function detected : {params}')
        
        decoded_params['from']= one_trace['from']
        decoded_params['token_addr']= one_trace['to']

        # get token symbol and denominator
        symbol, decimal = self.get_erc20_denom(decoded_params['token_addr'])
        decoded_params['symbol'] = symbol
        decoded_params['decimal'] = decimal
        
        if('amount' in params):
            decoded_params['value']=  int(params['amount'])/10**int(decimal)
        elif('_amount' in params):
            decoded_params['value']=  int(params['_amount'])/10**int(decimal)
        
        elif('wad' in params):
            decoded_params['value']=  int(params['wad'])/10**int(decimal)
        elif('_wad' in params):
            decoded_params['value']=  int(params['_wad'])/10**int(decimal)
        
        elif('value' in params):
            decoded_params['value']=  int(params['value'])/10**int(decimal)
        elif('_value' in params):
            decoded_params['value']=  int(params['_value'])/10**int(decimal)
        elif('rawAmount' in params):
            decoded_params['value']=  int(params['rawAmount'])/10**int(decimal)
        
        
        else:
            raise ValueError(f'new syntax for transfer function detected : {params}')
        
        
            
        decoded_params['function'] = decoded
        return decoded_params

    def transferFrom_handler(self, decoded, params, one_trace):
        decoded_params= {}

        # _sender, _recipient, _amount case
        if('_sender' in params):
            decoded_params['from']= params['_sender']
            decoded_params['to'] = params['_recipient']
        elif('sender' in params):
            decoded_params['from']= params['sender']
            decoded_params['to'] = params['recipient']
        # src, dst, wad case
        elif('_src' in params):
            decoded_params['from']= params['_src']
            decoded_params['to'] = params['_dst']

        # src, dst, wad case
        elif('src' in params):
            decoded_params['from']= params['src']
            decoded_params['to'] = params['dst']
        elif('_from' in params):
            decoded_params['from']= params['_from']
            decoded_params['to'] = params['_to']
        elif('from' in params):
            decoded_params['from']= params['from']
            decoded_params['to'] = params['to']


        
        else:
            raise ValueError(f'new syntax for transferFrom function detected : {params}')

        decoded_params['token_addr']= one_trace['to']

        # get token symbol and denominator
        symbol, decimal = self.get_erc20_denom(decoded_params['token_addr'])
        decoded_params['symbol'] = symbol
        decoded_params['decimal'] = decimal
        
        if('_sender' in params):
            decoded_params['value']=  int(params['_amount'])/10**int(decimal)
        elif('sender' in params):
            decoded_params['value']=  int(params['amount'])/10**int(decimal)
        elif('_src' in params):
            decoded_params['value']=  int(params['_wad'])/10**int(decimal)
        elif('src' in params):
            decoded_params['value']=  int(params['wad'])/10**int(decimal)
        elif('_from' in params):
            decoded_params['value']=  int(params['_value'])/10**int(decimal)
        elif('from' in params):
            decoded_params['value']=  int(params['value'])/10**int(decimal)
        elif('rawAmount' in params):
            decoded_params['value']=  int(params['rawAmount'])/10**int(decimal)
        
        else:
            raise ValueError(f'new syntax for transferFrom function detected : {params}')
        
        
        
        decoded_params['function'] = decoded

        return decoded_params



    def query_public_library(self, hex_signature):
        url = f"https://www.4byte.directory/api/v1/signatures/?hex_signature={hex_signature}"
        
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if 'results' in data and len(data['results']) > 0:
                logger.info(f'fetched function/event signature for the hex signature : {hex_signature}')
                return data['results'][0]['text_signature']
        logger.info(f'no matching text signature for {hex_signature}')
        return None
    
    def public_library_check(self, hex_input):
        # get first 8 hex digits (4 bytes) + 2 (0x)
        hex_signature = hex_input[:10] 
        text_signature = self.query_public_library(hex_signature)
        if(text_signature is None):
            logger.error(f'input hex : {hex_signature} was not found in the public library. Input cannot be decoded')
            decoded = hex_signature
        else: 
            decoded = text_signature
        return decoded
    
    def get_contract_abi(self, addr:str, ETHERSCAN_API=None):
        
        # make/check local cache 
        cached_file = f"{self.ET_root}/bytecode/{addr}.txt"
        if os.path.exists(cached_file):
            with open(cached_file, 'r') as infile:
                code = json.load(infile)
                print(f"using cached bytecode for {addr}")
            
        else:
            print(f"fetching bytecode for {addr}")
            code = self.w3.eth.get_code(addr) # expensive when using rpc services 

            self.check_dir(f"{self.ET_root}/bytecode")
            with open(cached_file, 'w') as outfile:
                json.dump(code.hex(), outfile)
                print('bytecode saved')


        # Check if the address is a contract account
        if code == '0x':
            print(f"{addr} is an EOA")
            return None, 'eoa'
        else:
            print(f"{addr} is a contract. Fetching abi...")
            contract_abi = self.get_abi(addr, ETHERSCAN_API)
            return contract_abi, 'contract'
        
    def get_abi(self, contract_addr:str, ETHERSCAN_API=None, contract_type=None):
        # check its its cached/called before
        cached_file =  f"{self.ET_root}/abis/{contract_addr}.txt"
        if os.path.exists(cached_file):
            with open(cached_file, 'r') as infile:
                abi_result = json.load(infile)
                if RETRY_UNVERIFIED==False and abi_result=='Contract source code not verified':
                    logger.info(f'ABI fetching step is ignored for {contract_addr} as previous attempts were not successful. In case you want to force fetching please set global var RETRY_UNVERIFIED to True in Eth_ETL.py')
                    return 
                else:
                    print("using cached abi")
                    return abi_result
        
        url = f"https://api.etherscan.io/api?module=contract&action=getabi&address={contract_addr}&apikey={ETHERSCAN_API}"
        response = requests.get(url)
        res = response.json()


        if res['status'] == '1':
            self.check_dir(f"{self.ET_root}/abis")
            with open(cached_file, 'w') as outfile:
                json.dump(res['result'], outfile)
                print('abi saved')

            return str(res['result'])
            
        else:
            print(f"Error: {res['message']} Result: {res['result']}")    
            self.check_dir(f"{self.ET_root}/abis")
            with open(cached_file, 'w') as outfile:
                json.dump(res['result'], outfile)
                print(f'There was an error when fetching abi for {contract_addr}, saved the error msg in abi folder')
            
            return # contract cannot be initialized with abi (could use function signatures for targeted approach)
    
        
    def abi_handler_addr_pos(self, search_addr, ETHERSCAN_API=None):
            # check if the interacting address is a contract
        search_addr = Web3.to_checksum_address(search_addr)
        contract_abi, verdict = self.get_contract_abi(search_addr, ETHERSCAN_API)
        if(contract_abi is None):
            print(f"ABI for the contract {search_addr} is not recoverable.")
            
        return contract_abi, verdict
        






