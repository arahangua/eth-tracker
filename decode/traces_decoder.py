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
from functools import wraps
import time
import ast

logger = logging.getLogger(__name__)
        

# functions for handling requests
def retry_on_not_200(max_retries=10, delay=2):
    """
    Decorator to retry a function that makes an HTTP request if a 503 status code is returned.

    :param max_retries: Maximum number of retry attempts.
    :param delay: Delay between retries in seconds.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                response = func(*args, **kwargs)
                if response.status_code == 200:
                    try:
                        response_json = response.json()
                        # Check for any error code in the response
                        if 'error' in response_json and 'code' in response_json['error']:
                            print(f"Attempt {attempt + 1} of {max_retries} encountered an error. Retrying in {delay} seconds...")
                        else:
                            return response  # Successful request with no error
                    except ValueError:
                        # Response is not JSON or does not have expected structure
                        return response
                print(f"Attempt {attempt + 1} of {max_retries} failed with status {response.status_code}. Retrying in {delay} seconds...")
                time.sleep(delay)
            print("Max retries reached. Request failed.")
            return None
        return wrapper
    return decorator


#gloabal variables

MAX_TRIES = 10
TIME_DELAY = 2 # seconds


# now = datetime.datetime.now()
# # Format the date to 'day-month-year'
# DATE = now.strftime('%d%m%y')
RETRY_UNVERIFIED=False

# function parameter patterns

# for transfer functions
VALUE_PATTERNS = ['_tokenAmount', 'money', '_transferTokensWithDecimal', 'amount256', '_val', '_token', 'IODflowAmount', 'tokenId_', 'id', '_id', 'tokenID', '_tokens', 'numTokens', 'share', 'Amount', 'amount', '_amount', 'amount_', 'amt', 'wad', '_wad', 'value', '_value','rawAmount','tokens','tokenId', '_tokenId']
TO_PATTERNS = ['findingAllRecipient', 'buyer', 'recipient', '_recipient', 'recipient_', 'dst', '_dst', 'dst_', 'to', '_to', 'to_', 'toAddress', 'target', 'receiver', '_receiver']
FROM_PATTERNS = ['_holder', 'owner', '_sender', 'sender', 'sender_', '_src', 'src', 'src_','_from', 'from', 'from_', 'holder', 'spender']
        
# below contracts do not follow the usual ERC20 format but have 'transfer' functions        
IRREGULAR_CONTRACTS = ['0xc385e90da38f8798f5a5512d415a13c87e0d6265', #BenDAO
                       '0x75a8b7c0b22d973e0b46cfbd3e2f6566905aa79f', #rarible erc1155
                       '0xbb7829bfdd4b557eb944349b2e2c965446052497', #rarible erc721
                       ]

TRANSFER_FUNC_SIGS = ['transfer',
                      '0x23b872dd', #transferFrom
                      '0xa9059cbb' #transfer
                      ]


### util methods. handles time conversions and simple path handling.

class Transfer_Decoder():
    def __init__(self, eth_tracker_loc, w3, API, DATE):
        self.w3 = w3
        self.apis= API
        self.ET_root = eth_tracker_loc
        self.DATE = DATE
    
    @retry_on_not_200(max_retries=MAX_TRIES, delay=TIME_DELAY)
    def get_url(self, url):
        response = requests.get(url,timeout=None)
        return response


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

    #csv_file = args.exported_file
    #search_str= args.search_keyword
    #use_known_pattern = args.transfer_func_patterns
        
    
    # main function that handles different transfer functions
    def decode_trace_csv(self, csv_file, parent_dir, search_str='transfer', use_known_pattern=True):
        
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
            
            init_addr = init_row['from'].squeeze()
            init_contract = init_row['to'].squeeze()

            # sort traces in a chronological order if necessary, *already sorted in a chronological order (miner set sequence) atm.
            target_concat = pd.DataFrame()
            if('transfer' in search_str):
                for search_sig in TRANSFER_FUNC_SIGS:
                    targets = tx_df[tx_df['decoded'].str.contains(search_sig)]
                    target_concat=pd.concat([target_concat, targets])
            
            # re-assign it for the downstream 
            targets = target_concat

            # rest = self.sort_trace_rows(rest) 

            # decode each row
            
            for ii, row in targets.iterrows():
                decoded = {}
                decoded['init_address'] = init_addr
                decoded['init_contract'] = init_contract
                decoded['blockNumber'] = row['blockNumber']
                decoded['tx_pos'] = row['transactionPosition']
                decoded['callType'] = row['callType']

                # some contracts are considered irregular already from the glance of etherscan.
                if(row['to'].lower() in IRREGULAR_CONTRACTS):
                    row_decoded = 'irregular_contract'
                else:
                    row_decoded, params = self.trace_decoding_handler(row, use_known_pattern = use_known_pattern)
                if(row_decoded is not None):
                    if(row_decoded == 'irregular_contract'):
                        # save this contract for later inspection
                        txt_name = f'{parent_dir}/irregular_contracts.txt'
                        irregular= row['to']
                        with open(txt_name, "a") as f:
                            f.write(irregular + "\n") 
                        logger.info(f'saved(appended) the address of the irregular contract ({irregular}) to {txt_name}')
                    else:
                        row_decoded['params'] = str(params)
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
        # if function name and params are same then we drop the one

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
            # padded = Web3.toHex(self.w3.eth.get_storage_at(contract_addr, "0x7050c9e0f4ca769c69bd3a8ef740bc37934f8e2c036e5a723fd8ee048ed3f8c3"))
            # padded = padded[-42:]
            # impl_addr = '0x' + padded[2:]
            with open(f"./proxy_mapping/{contract_addr}.txt", 'r') as infile:
                impl_addr = json.load(infile)
            
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

    def check_erc_type(self, contract_addr):
        # known constants for erc165 supportsinterface check
        # dict 
        id_dict = {}
        id_dict['erc721'] = '0x80ac58cd'
        id_dict['erc1155'] = '0xd9b67a26'
        id_dict['erc777'] = '0xe58e113c'

        supports_interface_fn_signature = self.w3.keccak(text='supportsInterface(bytes4)').hex()[0:10]
        
        supports = 'other'
        for k,v in id_dict.items():
            call_obj = {
                'to': contract_addr,
                'data': supports_interface_fn_signature + id_dict[k].rjust(64, '0')
            }
            result = self.w3.eth.call(call_obj)
            supports_check = result.hex() == '0x' + '1'.rjust(64, '0')
            print(f"The contract at {contract_addr} supports {k}: {supports_check}")
            
            # if we get a hit
            if(supports_check):
                if(supports=='other'):
                    supports = k
                else: # just line up strings
                    supports = supports + ',' + k
                
        # return supporting standards
        
        return supports

    
        
    def trace_decoding_handler(self, one_trace, use_known_pattern):
        contract_addr = one_trace['to']
        hex_input = one_trace['input']
        trace_value = one_trace['value']
        
        
        contract_addr = Web3.to_checksum_address(contract_addr) 


        # first check if ABI exists on Etherscan
        contract_abi, verdict = self.abi_handler_addr_pos(contract_addr, self.apis['ETHERSCAN_API'])
       
        # try to decode the input using ABI
        params = 'unused' # will be overwritten if ABI decoding was successful
        if(verdict=='contract'):
            # check if the contract implements erc20 or erc721 by using erc165 supportsInterface(bytes4) method
            # erc_type = self.check_erc_type(contract_addr)
            
            
            
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
                    if(type(params)==dict): # for normal decoded parameters
                        decoded = func.function_identifier
            # if input is 0x, need to check a value. If value is also 0x then likely a fallback function and if value is not 0x then likely a unwrapping (ether transfer) is happening.
            else:
                if(len(trace_value)>2):
                    eth_convert = int(trace_value, 16)/10**18
                    logger.info(f'likely a unwrapping event (ether transfer) {eth_convert:.3f} ETH')
                    decoded = 'ether transfer(contract)'
                   
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
                decoded = 'ether transfer(EOA)'

        # prepare decoded variable from the given the above case handling 
        decoded_params = self.decode_input_from_signature(decoded, params, one_trace, use_known_pattern)
        
        
        return  decoded_params, params
    
    def get_proxy_mapping(self, addr:str, ETHERSCAN_API=None):
            
        # make/check local cache 
        cached_file = f"./proxy_mapping/{addr}.txt"
        if os.path.exists(cached_file):
            with open(cached_file, 'r') as infile:
                impl_addr = json.load(infile)
                if(impl_addr == '0x0000000000000000000000000000000000000000'):
                    if not(os.path.exists(f"./verified_source/{addr}.txt")):
                        logger.info(f"cached file was found but checking verified source code from etherscan as there was no previous attempt.")
                        res = self.get_etherscan_source_code(addr,ETHERSCAN_API)
                        impl_addr = res[0]['Implementation']
                        logger.info(f'saving proxy mapping for {addr}')
                        self.check_dir(f"./proxy_mapping")
                        with open(cached_file, 'w') as outfile:
                            json.dump(impl_addr, outfile)
                else:
                    logger.info(f"using cached bytecode (implementation contract) {impl_addr}")
                
        else:
            # check etherscan for implementation address
            res = self.get_etherscan_source_code(addr,ETHERSCAN_API)
            impl_addr = res[0]['Implementation']

            if(len(impl_addr)==42):
                logger.info(f"found a valid impl addresss from etherscan verified source codes")

            else:
                # for openzeppelin upgradable template
                padded = Web3.toHex(self.w3.eth.get_storage_at(addr, "0x7050c9e0f4ca769c69bd3a8ef740bc37934f8e2c036e5a723fd8ee048ed3f8c3"))
                padded = padded[-42:]
                impl_addr = '0x' + padded[2:]

                if(impl_addr!='0x0000000000000000000000000000000000000000'):
                    logger.info(f"found a valid impl addresss from openzeppelin upgradable template")
                else:
                    # check for erc-1967 case
                    padded = Web3.toHex(self.w3.eth.get_storage_at(addr, "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"))
                    padded = padded[-42:]
                    impl_addr = '0x' + padded[2:]
                    
                    if(impl_addr!='0x0000000000000000000000000000000000000000'):
                        logger.info(f"found a valid impl addresss through erc-1967 slot check")
                    else:
                        # check UUPS
                        abi = """
                                [{
                                    "inputs": [],
                                    "name": "getImplementation",
                                    "outputs": [{"internalType": "address", "name": "", "type": "address"}],
                                    "stateMutability": "view",
                                    "type": "function"
                                }]
                                """

                        contract = self.w3.eth.contract(address=addr, abi=abi)
                        try:
                            impl_addr = contract.functions.getImplementation().call()
                            print(f"The implementation address is: {impl_addr}, {addr} is likely a UUPS contract")
                        except Exception as e:
                            print(f"Error retrieving implementation address: {e}, {addr} is likely not a UUPS contract")

                            # finally check for diamond cut (skipping for now as we don't know which one is the right impl_addr)




            # in case, needed we can check the value of impl_addr here.

            logger.info(f'saving proxy mapping for {addr}')
            self.check_dir(f"./proxy_mapping")
            with open(cached_file, 'w') as outfile:
                json.dump(impl_addr, outfile)
                
        return impl_addr
    
    def decode_input(self, hex_input, contract_addr, contract_abi):
        contract = self.w3.eth.contract(address=contract_addr, abi=contract_abi)

        # check if it is a proxy contract 
        impl_check = 'implementation' in contract_abi.lower()
        upgrade_check = 'upgrade' in contract_abi.lower()
        verdict = impl_check and upgrade_check
        if(verdict):
            logger.info(f'found a proxy contract ({contract_addr}). Fetching the implementation function to get the matching contract (for now only openzeppelin upgradable proxy is handled.)')
            # somehow nowadays proxy contracts prevent clients from calling implementation function directly.
            #implementation_address = contract.functions.implementation().call()

            # check first if there is cached data ("get_storage_at" can be expensive in terms of compute unit)
            impl_addr = self.get_proxy_mapping(contract_addr, ETHERSCAN_API=self.apis['ETHERSCAN_API'])

            # for openzeppelin upgradable template
            # padded = Web3.toHex(self.w3.eth.get_storage_at(contract_addr, "0x7050c9e0f4ca769c69bd3a8ef740bc37934f8e2c036e5a723fd8ee048ed3f8c3"))
            # padded = padded[-42:]
            # impl_addr = '0x' + padded[2:]
            
            if(impl_addr=='0x0000000000000000000000000000000000000000'):
                logger.info(f'found a proxy but {contract_addr} is not using openzeppelin upgradable contract... moving on')
                func = hex_input[:10]
                params = 'unknown_proxy' 
                return func, params

            else:
                logger.info(f'found a proxy contract that probably is an upgradable contract')
                impl_addr = Web3.to_checksum_address(impl_addr)
                contract_abi, verdict = self.get_contract_abi(impl_addr, ETHERSCAN_API=self.apis['ETHERSCAN_API'])
                contract = self.w3.eth.contract(address=contract_addr, abi=contract_abi)
        
        func, params = contract.decode_function_input(hex_input)

        return func, params
    # not used for now.
    def parse_dtype_from_sig(self, text_signature):
        pass

    def get_symbol_decimal_erc20(self, contract_address):
        contract = self.prep_contract(contract_address)
        try:
            # get symbol 
            symbol = contract.functions.symbol().call()

            # get denominator
            decimal = contract.functions.decimals().call()

            call_results= {}
            if(type(symbol)==bytes): # MKR does this...
                cleaned_bytes = symbol.rstrip(b'\x00')  # Remove trailing null bytes
                symbol = cleaned_bytes.decode('utf-8')
            if(symbol is None):
                symbol = 'empty'
            call_results['symbol']=symbol
            call_results['decimal']=decimal
                 
        except Exception as e:
            print(e)
            logger.error(f'suspecting a faulty erc20 contract : {contract_address} (no symbol or decimal function implemented)')
            logger.error('labeling as faulty token with decimal point of 18')
            call_results= {}
            call_results['symbol']='Faulty'
            call_results['decimal']= 18

        return call_results

    def get_erc20_denom(self, contract_address):
        

        # check cache 
        cache_root = f'{self.ET_root}/calls'
        cached_file = f"{self.ET_root}/calls/{contract_address}.txt"
        
        if os.path.exists(cached_file):
            with open(cached_file, 'r') as infile:
                call_results = json.load(infile)
                print(f"using cached call results for {contract_address}")
                if(type(call_results['symbol'])!=bytes): # checking for problems in locally-cached files

                    return call_results['symbol'], call_results['decimal']
                else:
                    call_results = self.get_symbol_decimal_erc20(contract_address)
                    # save results 
                    self.check_dir(cache_root)
                    with open(cached_file, 'w') as outfile:
                        json.dump(call_results, outfile)
                        print(f"call data saved for {contract_address}")

        else:
            
            call_results = self.get_symbol_decimal_erc20(contract_address)
            # save results 
            self.check_dir(cache_root)
            with open(cached_file, 'w') as outfile:
                json.dump(call_results, outfile)
                print(f"call data saved for {contract_address}")

        # if(contract_address.lower()=='0xa2327a938febf5fec13bacfb16ae10ecbc4cbdcf'):
        #     symbol = 'USDC'
        #     decimal = 6

        return call_results['symbol'], call_results['decimal']

    # all ether transfers and standard erc20 transfers will be handled.
    def decode_input_from_signature(self, decoded, params, one_trace, use_known_pattern=True):
        
        # erc20 transfer
       
        if(decoded == 'transfer'):
            # check abnormal parameter length
            if(len(params)<2):
                logger.error(f'found abnormal parameter length : {params}')
                decoded_params= None
                return decoded_params

            if(use_known_pattern):
                decoded_params = self.transfer_handler(decoded, params, one_trace)
            else:
                decoded_params = self.transfer_handler_unsafe(decoded, params, one_trace)
        elif(decoded == 'transferFrom'):
            # check abnormal parameter length
            if(len(params)<2):
                logger.error(f'found abnormal parameter length : {params}')
                decoded_params= None
                return decoded_params

            if(use_known_pattern):
                decoded_params = self.transferFrom_handler(decoded, params, one_trace)
            else:
                decoded_params = self.transferFrom_handler_unsafe(decoded, params, one_trace)
          
        elif(decoded =='ether transfer(EOA)'):
            decoded_params= {}
            decoded_params['from']= one_trace['from']
            decoded_params['to'] = one_trace['to']
            decoded_params['token_addr']= 'ETH'
            decoded_params['symbol'] = 'ETH'
            decoded_params['decimal'] = 18
            decoded_params['value']=   int(one_trace['value'], 16)/10**18
            decoded_params['function'] = decoded

        elif(decoded=='ether transfer(contract)'):
            decoded_params= {}
            decoded_params['from']= one_trace['from']
            decoded_params['to'] = one_trace['to']
            decoded_params['token_addr']= 'ETH'
            decoded_params['symbol'] = 'ETH'
            decoded_params['decimal'] = 18
            decoded_params['value']=   int(one_trace['value'], 16)/10**18
            decoded_params['function'] = decoded

      
        else:
            logger.error(f'found an unhandled function (or signature): {decoded}')
            decoded_params= None
            

        return decoded_params
    def transfer_handler_unsafe(self, decoded, params, one_trace):
        # assumption : param dict is always in to, value sequence
        decoded_params= {}

        key_list = list(params.keys())
        logger.info(f'the transfer function is using the following keys : {key_list}')
        decoded_params['from']= one_trace['from']
        decoded_params['to'] = params[key_list[0]]
        decoded_params['token_addr']= one_trace['to']
        # get token symbol and denominator
        symbol, decimal = self.get_erc20_denom(decoded_params['token_addr'])
        decoded_params['symbol'] = symbol
        decoded_params['decimal'] = decimal
        
        token_amt = params[key_list[1]]
        try:
            int_casted= int(token_amt)
        except:
            logger.error(f'the token_amt :{token_amt} was not castable to integer. Skipping the current row')
            return None

        decoded_params['value'] =  int(token_amt)/10**int(decimal)
        decoded_params['function'] = decoded
        return decoded_params
    
    def transfer_handler(self, decoded, params, one_trace):
        decoded_params= {}
        to_check = False
        for pattern in TO_PATTERNS:
            if(pattern in params):
                decoded_params['to'] = params[pattern]
                to_check = True
        # check if to is set correctly
        if(not(to_check)):
            logger.error(f'the current function parameters do not match predefined \'to\' patterns, skipping the current line and saving this contract address')
            decoded_params = 'irregular_contract'
            return decoded_params
        
        decoded_params['from']= one_trace['from']
        decoded_params['token_addr']= one_trace['to']

        # get token symbol and denominator
        symbol, decimal = self.get_erc20_denom(decoded_params['token_addr'])
        decoded_params['symbol'] = symbol
        decoded_params['decimal'] = decimal

        value_check=False
        for pattern in VALUE_PATTERNS:
            if(pattern in params):
                decoded_params['value'] = int(params[pattern])/10**int(decimal)
                value_check = True
        
        nominal = to_check and value_check
        
        if(not(nominal)):
            raise ValueError(f'new syntax for transfer function detected : {params}')
        
        
            
        decoded_params['function'] = decoded
        return decoded_params
    
    def transferFrom_handler_unsafe(self, decoded, params, one_trace):
        # assumption : param dict is always in from, to, value sequence
        decoded_params= {}
        
        key_list = list(params.keys())     
        logger.info(f'the transferFrom function is using the following keys : {key_list}')
        if(len(key_list)>3):
            try:
                params.pop('token')
            except:
                logger.error(f'we are getting more than 3 args for transferfrom function and the above keys are not widely used. Skipping the current row.')
                return None
        key_list = list(params.keys())
        
        decoded_params['from'] = params[key_list[0]]
        decoded_params['to'] = params[key_list[1]]
        decoded_params['token_addr']= one_trace['to']
        # get token symbol and denominator
        symbol, decimal = self.get_erc20_denom(decoded_params['token_addr'])
        decoded_params['symbol'] = symbol
        decoded_params['decimal'] = decimal
        
        token_amt = params[key_list[2]]
        decoded_params['value'] =  int(token_amt)/10**int(decimal)
        decoded_params['function'] = decoded
        return decoded_params


    def transferFrom_handler(self, decoded, params, one_trace):
        # logger.info(f'{params}')
        decoded_params= {}
        from_check=False
        for pattern in FROM_PATTERNS:
            if(pattern in params):
                decoded_params['from'] = params[pattern]
                from_check = True
        
        to_check = False
        for pattern in TO_PATTERNS:
            if(pattern in params):
                decoded_params['to'] = params[pattern]
                to_check = True
        
        # check if to is set correctly
        if(not(to_check)):
            logger.error(f'the current function parameters do not match predefined \'to\' patterns, skipping the current line and saving this contract address')
            decoded_params = 'irregular_contract'
            return decoded_params
        
        decoded_params['token_addr']= one_trace['to']

        # get token symbol and denominator
        symbol, decimal = self.get_erc20_denom(decoded_params['token_addr'])
        decoded_params['symbol'] = symbol
        decoded_params['decimal'] = decimal
        
        value_check=False
        for pattern in VALUE_PATTERNS:
            if(pattern in params):
                decoded_params['value'] = int(params[pattern])/10**int(decimal)
                value_check = True
        
        
        nominal = from_check and to_check and value_check
        check_sum = np.sum([from_check,to_check, value_check])
        if(not(nominal)):
            if(check_sum==2): # e.g. Morphous contract
                edge_case_contract = one_trace['to']
                logger.error(f'for this transfer event (contract address : {edge_case_contract}), we found a derivative function of ERC20 transfer function parameters : {params}, skipping this transfer event (cannot be interpreted normally)')
                decoded_params = 'irregular_contract'
                return decoded_params

            
            raise ValueError(f'new syntax for transfer function detected : {params}')
        
        
        
        decoded_params['function'] = decoded

        return decoded_params



    def query_public_library(self, hex_signature):
        url = f"https://www.4byte.directory/api/v1/signatures/?hex_signature={hex_signature}"
        
        response = self.get_url(url)
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
        
    def get_etherscan_source_code(self, contract_addr:str, ETHERSCAN_API=None):
        cached_file =  f"{self.ET_root}/verified_source/{contract_addr}.txt"
        if os.path.exists(cached_file):
            with open(cached_file, 'r') as infile:
                sc_result = json.load(infile)
                if RETRY_UNVERIFIED==False and sc_result=='Contract source code not verified': #--> abi not usable
                    logger.info(f'source code fetching step is ignored for {contract_addr} as previous attempts were not successful. In case you want to force fetching please set global var RETRY_UNVERIFIED to True in Eth_ETL.py')
                    return 
                else:
                    if sc_result!='Max rate limit reached': # checks for previous rate limit problem
                        print("using cached source code")
                        return sc_result
        
        url = f"https://api.etherscan.io/api?module=contract&action=getsourcecode&address={contract_addr}&apikey={ETHERSCAN_API}"
        response = self.get_url(url)
        res = response.json()

        if res['status'] == '1':
            self.check_dir(f"{self.ET_root}/verified_source")
            with open(cached_file, 'w') as outfile:
                json.dump(res['result'], outfile)
                print('verified source code saved')

            return res['result']
            
        else:
            print(f"Error: {res['message']} Result: {res['result']}")    
            self.check_dir(f"{self.ET_root}/abis")
            with open(cached_file, 'w') as outfile:
                json.dump(res['result'], outfile)
                print(f'There was an error when fetching verified source code for {contract_addr}, saved the error msg in verified_source folder')
            
            return # contract cannot be initialized with abi (could use function signatures for targeted approach)


        
    def get_abi(self, contract_addr:str, ETHERSCAN_API=None, contract_type=None):
        # check its its cached/called before
        cached_file =  f"{self.ET_root}/abis/{contract_addr}.txt"
        if os.path.exists(cached_file):
            with open(cached_file, 'r') as infile:
                abi_result = json.load(infile)
                if RETRY_UNVERIFIED==False and abi_result=='Contract source code not verified': #--> abi not usable
                    logger.info(f'ABI fetching step is ignored for {contract_addr} as previous attempts were not successful. In case you want to force fetching please set global var RETRY_UNVERIFIED to True in Eth_ETL.py')
                    return 
                else:
                    if abi_result!='Max rate limit reached': # checks for previous rate limit problem
                        print("using cached abi")
                        return abi_result
        
        url = f"https://api.etherscan.io/api?module=contract&action=getabi&address={contract_addr}&apikey={ETHERSCAN_API}"
        response = self.get_url(url)
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

###################################################################################################
    
    # for decoding logs
    def decode_logs_csv(self, csv_file):
        csv_df = pd.read_csv(csv_file)
 
        # this concat df includes multiple tx positions in a chronological order.
        logger.info(f'decoding csv file : {csv_file}, number of logs (rows): {len(csv_df)}')        
        concat = pd.DataFrame() 
        for ii, log in csv_df.iterrows():
            
            # decode log data
            contract_addr = log['address']
            contract_abi = self.get_abi(contract_addr, self.apis['ETHERSCAN_API'])# Your contract ABI here
            if(contract_abi is None):
                logger.error(f'failed to fetch abi for {contract_addr}')
                logger.info(f'continuing')
                continue
            else:
                # create contract instance
                contract = self.w3.eth.contract(address=contract_addr, abi=contract_abi) 
                #get abi mapping (events)
                event_abi_map = self.generate_event_abi_map(contract_abi)
                #decode the log
                decoded_data = self._decode_log(log, event_abi_map, contract)
            
                # prep decoded result variable
                decoded = {}
                decoded['blockNumber'] = log['blockNumber']
                decoded['tx_hash'] = log['transactionHash']
                decoded['tx_index'] = log['transactionIndex']
                decoded['log_index'] = log['logIndex']
                decoded['contract_addr'] = log['address']
                decoded['event'] = decoded_data['event']
                decoded['decoded'] = str(dict(decoded_data['args']))

                decode_df = pd.DataFrame(decoded, index=[0])
                concat = pd.concat([concat, decode_df])
            
        
        concat = concat.reset_index(drop='index')
        
        return concat
    
    # Generate a dictionary mapping event signature hashes to event ABIs
    def generate_event_abi_map(self, contract_abi):
        json_abi = json.loads(contract_abi)
        event_abi_map = {}
        for abi_entry in json_abi:
            # print(abi_entry)
            if abi_entry['type'] == 'event':
                event_signature = f"{abi_entry['name']}({','.join([input['type'] for input in abi_entry['inputs']])})"
                event_signature_hash = self.w3.keccak(text=event_signature).hex()
                event_abi_map[event_signature_hash] = abi_entry    
        return event_abi_map                    
        
    def _decode_log(self, log, event_abi_map, contract):
        # Match the event signature hash
        event_signature_hash = ast.literal_eval(log['topics'])[0] if log['topics'] else None
        event_abi = event_abi_map.get(event_signature_hash)

        if event_abi:
            # Decode the log based on the matched event ABI
            log['topics'] = ast.literal_eval(log['topics'])
            log['topics'] = list(map(lambda x: bytes.fromhex(x[2:]),log['topics']))
            return contract.events[event_abi['name']]().process_log(log)
        else:
            return None

# import eth_utils
# eth_utils.abi.event_abi_to_log_topic(event_abi)
# hex_string = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
# # Convert hex string (without the '0x' prefix) to bytes
# byte_object = bytes.fromhex(hex_string[2:])
# print(byte_object)


#below for debugging purpose
'''
import argparse

filter_parser = argparse.ArgumentParser()
args = argparse.Namespace(search_keyword='transfer', exported_file= '/home/takim/work/eth-tracker/output/190923/trace_out/17932904/traced_out.csv', job_id= '0')
    
    
'''
    