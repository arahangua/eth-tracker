import os, sys
import json
import requests
import logging
import pandas as pd
import hexbytes
import utils
import datetime 
from web3 import Web3
import hashlib
import eth_abi 
from functools import wraps
import time
# now = datetime.datetime.now()
# # Format the date to 'day-month-year'
# DATE = now.strftime('%d%m%y')

#global switch for fetching abis 
RETRY_UNVERIFIED=False

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

# functions for handling empty logs
def retry_on_empty(max_retries=10, delay=2):
    """
    Decorator to retry a function that makes an getlog request again if the log is empty

    :param max_retries: Maximum number of retry attempts.
    :param delay: Delay between retries in seconds.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                response = func(*args, **kwargs)
                if len(response)>0:
                    return response
                print(f"Attempt {attempt + 1} of {max_retries} as empty logs were returned. Retrying in {delay} seconds...")
                time.sleep(delay)
            print("Max retries reached. Request failed.")
            return None
        return wrapper
    return decorator


MAX_TRIES = 10
TIME_DELAY = 2 # seconds



class Eth_tracker():
    def __init__(self, web3_instance, block_id, contracts, apis, DATE):
        self.w3 = web3_instance
        try:
            block_id = int(block_id)
        except:
            pass
        self.block_id = block_id
        self.contracts = contracts
        self.apis = apis
        self.DATE = DATE
        logger.info('eth etl class initialized')

    def fetch_blockinfo(self):

        blockinfo = self.w3.eth.get_block(self.block_id)
        logger.info(f'block fetched, block id: {self.block_id}')
        # print(blockinfo)
        
        return blockinfo
        
    @retry_on_not_200(max_retries=MAX_TRIES , delay=TIME_DELAY)
    def send_request(self, url, request_body):
        response = requests.post(url, json=request_body,timeout=None)
        return response
    
    @retry_on_not_200(max_retries=MAX_TRIES, delay=TIME_DELAY)
    def get_url(self, url):
        response = requests.get(url, timeout=None)
        return response


    def fetch_blocktrace(self, rpc_provider):
        
        url = f"{rpc_provider}"
        
        request={
                    "jsonrpc":"2.0",
                    "method":"trace_block",
                    "params":[str(self.block_id)],
                    "id":1
                }
        
        response = self.send_request(url, request)
        res = response.json()

        blocktrace = res['result']

        logger.info(f'traces for the block fetched, block id: {self.block_id}')
        
        return blocktrace
    
    
    def get_transactions(self, blockinfo):    
        tx_hashes = blockinfo['transactions']
        collect=pd.DataFrame()
        collect_keys=['hash', 'from', 'to', 'input']
        for tx_hash in tx_hashes:
            summary ={}
            tx = self.w3.eth.get_transaction(tx_hash)
            # print(tx)
            for key in collect_keys:
                if(key=='hash'):
                    summary[key]=tx[key].hex()
                elif((key=='to') & (key not in tx.keys())):
                    logger.info(f'found a contract creation event for tx(hash) : {tx_hash.hex()}')
                    summary[key]='contract_created'
                else:
                    summary[key]=tx[key]
            
            summary = pd.DataFrame(summary, index=[0])
            collect=pd.concat([collect, summary])
        collect=collect.reset_index(drop='index') 
        logger.info(f"exported {len(collect)} transactions")
        return collect
    

    def get_trace_actions(self, blocktrace):    
        collect=pd.DataFrame()
        for action in blocktrace:
            summary ={}
            keys = action['action'].keys()
            for key in keys:
                summary[key]=action['action'][key]

            summary = pd.DataFrame(summary, index=[0])
            collect=pd.concat([collect, summary])
        collect=collect.reset_index(drop='index')
        logger.info(f"exported {len(collect)} transactions")
        return collect
    
### finding interacting contracts(traces)  + input data
    def find_interacting_traces(self, actions, blocktrace, ETHERSCAN_API=None, addr_pos='to'):
        for search_entry in self.contracts:
            logger.info(f'searching for contract {search_entry} in {addr_pos} position')
            # find contracts calling the target contract
            index = actions.loc[actions['to']==search_entry.lower()].index
            if(len(index)):
                logger.info(f"{len(index)} match(es) was(were) found for the contract {search_entry}")
                for ind in index:    
                    self.write_result_trace(search_entry, actions.iloc[ind,:], blocktrace[ind], addr_pos, ETHERSCAN_API=ETHERSCAN_API)
                    
            else:
                logger.info(f"no traces were calling the contract {search_entry}")
            
            
### finding interacting contracts + input data
    def find_interacting_contracts(self, collect, ETHERSCAN_API=None, addr_pos='to'):
        for search_entry in self.contracts:
            logger.info(f'searching for contract {search_entry} in {addr_pos} position')
            # find contracts calling the target contract
            subset = collect.loc[collect[addr_pos]==search_entry]
            if(len(subset)):
                logger.info(f"{len(subset)} match(es) was(were) found for the contract {search_entry}")
                # check if the interacting address is a contract
                for ind in range(len(subset)):
                    self.write_result_tx(search_entry, subset.iloc[ind,:], addr_pos, ETHERSCAN_API=ETHERSCAN_API)
                    
            else:
                logger.info(f"no contracts were calling the contract {search_entry}")


### finding interacting contracts adresses (no input data)
    def find_interacting_addrs(self, collect, addr_pos='to'):
        for search_entry in self.contracts:
            logger.info(f'searching for contract {search_entry} in {addr_pos} position')
            subset = collect.loc[collect[addr_pos]==search_entry]
            if(len(subset)):
                logger.info(f"{len(subset)} match(es) was(were) found for the contract {search_entry}")
                subset = subset[['hash', 'from', 'to']]
                self.write_result_addrs(search_entry, subset, addr_pos)
            else:
                logger.info(f"no interacting addresses were found for the contract {search_entry}")


    def get_proxy_mapping(self, addr:str, ETHERSCAN_API=None):

        # make/check local cache 
        cached_file = f"./proxy_mapping/{addr}.txt"
        if os.path.exists(cached_file):
            with open(cached_file, 'r') as infile:
                impl_addr = json.load(infile)
                logger.info(f"using cached bytecode (implementation contract) {impl_addr}")
                
        else:
             # for openzeppelin upgradable template
            padded = Web3.toHex(self.w3.eth.get_storage_at(addr, "0x7050c9e0f4ca769c69bd3a8ef740bc37934f8e2c036e5a723fd8ee048ed3f8c3"))
            padded = padded[-42:]
            impl_addr = '0x' + padded[2:]
            
            # in case, needed we can check the value of impl_addr here.

            logger.info(f'saving proxy mapping for {addr}')
            utils.check_dir(f"./proxy_mapping")
            with open(cached_file, 'w') as outfile:
                json.dump(impl_addr, outfile)
                
        return impl_addr

            

    def get_contract_abi(self, addr:str, ETHERSCAN_API=None):
        
        # make/check local cache 
        cached_file = f"./bytecode/{addr}.txt"
        if os.path.exists(cached_file):
            with open(cached_file, 'r') as infile:
                code = json.load(infile)
                print(f"using cached bytecode for {addr}")
            
        else:
            print(f"fetching bytecode for {addr}")
            code = self.w3.eth.get_code(addr) # expensive when using rpc services 

            utils.check_dir(f"./bytecode")
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
                logger.info(f'found a proxy contract that probably is using openzeppelin upgradable contract')
                impl_addr = Web3.to_checksum_address(impl_addr)
                contract_abi, verdict = self.get_contract_abi(impl_addr, ETHERSCAN_API=self.apis['ETHERSCAN_API'])
                contract = self.w3.eth.contract(address=contract_addr, abi=contract_abi)
        
        func, params = contract.decode_function_input(hex_input)

        return func, params
    
    def write_result_tx(self, search_entry, collect_subset, addr_pos='to', ETHERSCAN_API=None):
        write ={}
        write['block_identifier']=self.block_id
        write['contract_address']=search_entry
        
        contract_abi, verdict = self.abi_handler_addr_pos(collect_subset['to'], ETHERSCAN_API)


        if(contract_abi is None):
            inner={}
            inner['transaction_hash']=collect_subset['hash']
            inner['from']=collect_subset['from']
            inner['to']= collect_subset['to']
            write['output']=inner
        else:
            func, params = self.decode_input(collect_subset['input'], collect_subset['to'], contract_abi)
            inner={}
            inner['transaction_hash']=collect_subset['hash']
            inner['from']=collect_subset['from']
            inner['to']= collect_subset['to']
            inner['decoded']={}
            if(params=='unknown_proxy'):
                inner['decoded']['function_name']=func
            elif(params=='ABI_reading_problem'):
                inner['decoded']['function_name']=func
            else:
                inner['decoded']['function_name']=func.function_identifier
            inner['decoded']['values']=params
            write['output']=inner
        
        utils.check_dir(f"./output/{self.DATE}/tx/")
        with open(f"./output/{self.DATE}/tx/block_id_{self.block_id}_{addr_pos}_contract_addr_{search_entry}_tx_{inner['transaction_hash']}.txt", 'w') as outfile:
            json.dump(write, outfile)
        logger.info(f"results (contracts that were calling the target contract) for {self.block_id} and the contract {search_entry} was successfully saved")

    def write_result_addrs(self, search_entry, subset, addr_pos):
        utils.check_dir(f"./output/{self.DATE}/addrs")
        subset.to_csv(f"./output/{self.DATE}/addrs/block_id_{self.block_id}_{addr_pos}_contract_addr_{search_entry}.csv", index=False)
        logger.info(f"results for the contract {search_entry} was successfully saved")


    def write_result_trace(self, search_entry, action_subset, blocktrace_subset, addr_pos='to', ETHERSCAN_API=None):
        write ={}
        write['block_identifier']=self.block_id
        write['contract_address']=search_entry
        
        inner={}
        inner['from']=action_subset['from']
        inner['to']=action_subset['to']
        inner['callType']=action_subset['callType']
        inner['gas']=action_subset['gas']
        inner['value']=action_subset['value']
        inner['blockHash']=blocktrace_subset['blockHash']
        inner['blockNumber']=blocktrace_subset['blockNumber']
        inner['subtraces']=blocktrace_subset['subtraces']
        inner['transactionHash']=blocktrace_subset['transactionHash']
        inner['transactionPosition']=blocktrace_subset['transactionPosition']
        inner['gasUsed']=blocktrace_subset['result']['gasUsed']
        
        contract_abi, verdict = self.abi_handler_addr_pos(action_subset['to'], ETHERSCAN_API)


        if(contract_abi is None):    
            write['trace']=inner
        else:
            try:
                input = action_subset['input']
                func, params = self.decode_input(input, search_entry, contract_abi)
                logger.info(f'decoded inputs for the trace (index: {action_subset.name} of block: {self.block_id})')
                inner['decoded']={}
                inner['decoded']['input']={}
                if(params=='unknown_proxy'):
                    inner['decoded']['input']['function_name']=func
                else:            
                    inner['decoded']['input']['function_name']=func.function_identifier
                inner['decoded']['input']['values']=params
            except Exception as e:
                logger.error(f"input could not be decoded. input: {input}  \n tx_hash: {inner['transactionHash']}, skipping the decoding of the input. Error raised : {e}")

            if(blocktrace_subset['result']['output']!='0x'):
                output = blocktrace_subset['result']['output']
                try:
                    func, params = self.decode_input(output, search_entry, contract_abi)
                    logger.info(f'decoded outputs for the trace (index: {action_subset.name} of block: {self.block_id})')
                    inner['decoded']['output']={}
                    if(params=='unknown_proxy'):
                        inner['decoded']['output']['function_name']=func
                    elif(params=='ABI_reading_problem'):
                        inner['decoded']['output']['function_name']= func
                    else:    
                        inner['decoded']['output']['function_name']=func.function_identifier
                    inner['decoded']['output']['values']=params
                
                except Exception as e:
                    logger.error(f"output could not be decoded. output: {output}  \n tx_hash: {inner['transactionHash']}, skipping the decoding of the output. Error raised : {e}")
                
            write['trace']=inner
        utils.check_dir(f"./output/{self.DATE}/traces")
        with open(f"./output/{self.DATE}/traces/block_id_{self.block_id}_{addr_pos}_contract_addr_{search_entry}_trace_index_{action_subset.name}.txt", 'w') as outfile:
            json.dump(write, outfile)
        logger.info(f"results (traces that were calling the target contract) for {self.block_id} and the contract {search_entry} was successfully saved")

    

    
    def get_abi(self, contract_addr:str, ETHERSCAN_API=None, contract_type=None):
        # check its its cached/called before
        cached_file = f"./abis/{contract_addr}.txt"
        if os.path.exists(cached_file):
            with open(cached_file, 'r') as infile:
                try:
                    abi_result = json.load(infile)
                    if RETRY_UNVERIFIED==False and abi_result=='Contract source code not verified': # --> abi not usable.
                        logger.info(f'ABI fetching step is ignored for {contract_addr} as previous attempts were not successful. In case you want to force fetching please set global var RETRY_UNVERIFIED to True in Eth_ETL.py')
                        return 
                    else:
                        if not('Max rate limit reached' in abi_result): # checks for previous rate limit problem
                            print("using cached abi")
                            return abi_result
                except Exception as e:
                    logger.error(e)
                    logger.error(f'saved ABI was not in a proper format for: {contract_addr}')

        url = f"https://api.etherscan.io/api?module=contract&action=getabi&address={contract_addr}&apikey={ETHERSCAN_API}"
        response = self.get_url(url)
        res = response.json()


        if res['status'] == '1':
            utils.check_dir(f"./abis")
            with open(cached_file, 'w') as outfile:
                json.dump(res['result'], outfile)
                print('abi saved')

            return str(res['result'])
            
        else:
            print(f"Error: {res['message']} Result: {res['result']}")    
            utils.check_dir(f"./abis")
            with open(cached_file, 'w') as outfile:
                json.dump(res['result'], outfile)
                print(f'There was an error when fetching abi for {contract_addr}, saved the error msg in abi folder')
            
            return # contract cannot be initialized with abi (could use function signatures for targeted approach)
    
    
    
    def abi_handler_addr_pos(self, search_addr, ETHERSCAN_API=None):
            # check if the interacting address is a contract
        search_addr = Web3.to_checksum_address(search_addr)
        contract_abi, verdict = self.get_contract_abi(search_addr, ETHERSCAN_API)
        if(contract_abi is None):
            logger.error(f"ABI for the contract {search_addr} is not recoverable.")
            
        return contract_abi, verdict
    
    @retry_on_empty(max_retries=MAX_TRIES, delay=TIME_DELAY)
    def get_logs_try(self, filter_params):
        logs = self.w3.eth.get_logs(filter_params)
        return logs


    def get_blocks_filter(self, args, search_addr):
        # check if its in checksum address
        search_addr = Web3.to_checksum_address(search_addr) 
        filter_params = self.make_filter(args, search_addr)
        #get logs
        logs = self.get_logs_try(filter_params)
        if(len(logs)>0):
            logger.info(f'got filter logs for the address : {search_addr}')
            # get target blocks
            target_blocks = self.extract_blocks_from_logs(logs)
            return target_blocks
        else:
            logger.error(f'returned logs were empty')
            return None
        
    def _get_logs_filter(self, args, search_addr):
        # check if its in checksum address
        search_addr = Web3.to_checksum_address(search_addr) 
        filter_params = self.make_filter(args, search_addr)
        #get logs
        logs = self.get_logs_try(filter_params)
        if(len(logs)>0):
            logger.info(f'got filter logs for the address : {search_addr}')
            # get target blocks
            formatted = self.format_logs(logs)
            return formatted
        else:
            logger.error(f'returned logs were empty')
            return None


    def extract_blocks_from_logs(self, logs):
        blocks=[]
        for log in logs:
            blocks.append(log['blockNumber'])
        blocks = list(set(blocks))
        return blocks
    
    def format_logs(self, logs):
        
        # Convert AttributeDict to regular dict and HexBytes to string
        converted_data = []
        for entry in logs:
            inner = {}
            for key, value in entry.items():
                if(isinstance(value, hexbytes.HexBytes)):
                    inner[key] = str(value.hex())
                elif(isinstance(value, list)):
                    for ii, val in enumerate(value):
                        if(isinstance(val, hexbytes.HexBytes)):
                            value[ii] = str(val.hex())
                        else:
                            value[ii] = str(val)
                    inner[key] = value
                
                else:
                    inner[key]= value
            converted_data.append(inner)

        # Convert to DataFrame
        df = pd.DataFrame(converted_data)
        return df
    
        
        

        
    def make_filter(self, args, search_addr):
        return {'fromBlock': int(args.start_block), 'toBlock': int(args.end_block), 'address': search_addr}
        
    def save_interim_filter_res(self, blocks_of_int, CONTRACTS_BK, args):
        hashed = self.get_hash_of_list(CONTRACTS_BK)
        interim_fol = f'./output/{args.start_block}_{args.end_block}/{hashed}/filter_intermediate'
        utils.check_dir(interim_fol)
        with open(f'{interim_fol}/target_blocks.json', 'w') as f:
            json.dump(blocks_of_int, f)
        logger.info('block list saved in the intermediate folder')
        with open(f'{interim_fol}/used_addrs.json', 'w') as f:
            json.dump(CONTRACTS_BK, f)
        logger.info('matching contract list saved in the intermediate folder')
        
        
    def get_hash_of_list(self, some_list):
        list_str = str(some_list).encode('utf-8')
        hash_object = hashlib.sha256(list_str)
        hex_dig = hash_object.hexdigest()
        return hex_dig


    def get_target_blocks(self, CONTRACTS_BK, args):
        # check if its cached
        hashed = self.get_hash_of_list(CONTRACTS_BK)
        cache_loc = f'./output/{args.start_block}_{args.end_block}/{hashed}/filter_intermediate/used_addrs.json'
        if(os.path.exists(cache_loc)):

            with open(cache_loc, 'r') as f:
                cache_contracts = json.load(f)
                cache_hashed = self.get_hash_of_list(cache_contracts)

            if(hashed==cache_hashed):
                logger.info('found previous cached data for valid blocklist')
                bl_list_loc = f'./output/{args.start_block}_{args.end_block}/{hashed}/filter_intermediate/target_blocks.json'
                with open(bl_list_loc, 'r') as f:
                    blocks_of_int = json.load(f)
            else: 
                blocks_of_int = self.collect_blocks(CONTRACTS_BK, args)

        else:
            blocks_of_int = self.collect_blocks(CONTRACTS_BK, args)
            
        
        return blocks_of_int
    
    def get_target_logs(self, CONTRACTS_BK, args):
        for search_addr in CONTRACTS_BK:
            logger.info(f'getting logs for the address : {search_addr}, starting block : {args.start_block} ending block : {args.end_block}')
            # check if its cached
            cache_root = f'./output/{self.DATE}/{args.start_block}_{args.end_block}/logs'
            cache_file = f'{cache_root}/{search_addr}.csv'
            
            if(os.path.exists(cache_file)):
                logger.info(f'logs for the address : {search_addr} was already exported for the given block range')
                
            else:
                target_logs = self._get_logs_filter(args, search_addr)
                
                if(target_logs is not None):
                    logger.info(f'found {len(target_logs)} log entries for addr : {search_addr}')
                    utils.check_dir(cache_root)
                    target_logs.to_csv(cache_file, index=False)
                    logger.info(f'exporting done.')
                else:
                    logger.error(f'no logs were found for the given range of blocks')
    
            
    
    
    def collect_logs(self, CONTRACTS_BK, args):
        logger.info('no cached logs exists for the current job')
        logs = []
        for search_addr in CONTRACTS_BK:
            logger.info(f'applying a filter for the address : {search_addr}')
            target_blocks = self.get_blocks_filter(args, search_addr)
            if(target_blocks is None):
                logger.error(f"returned logs were empty for address : {search_addr}")
            else:
                blocks_of_int = blocks_of_int + target_blocks
                blocks_of_int = list(set(blocks_of_int))
        if(args.save_blocklist):
            self.save_interim_filter_res(blocks_of_int, CONTRACTS_BK, args)
        
        return blocks_of_int 

    def collect_blocks(self, CONTRACTS_BK, args):
        logger.info(f'no cached blocklist exists for the current job')
        blocks_of_int= []
        for search_addr in CONTRACTS_BK:
            logger.info(f'applying a filter for the address : {search_addr}')
            target_blocks = self.get_blocks_filter(args, search_addr)
            if(target_blocks is None):
                logger.error(f"returned logs were empty for address : {search_addr}")
            else:
                blocks_of_int = blocks_of_int + target_blocks
                blocks_of_int = list(set(blocks_of_int))
        if(args.save_blocklist):
            self.save_interim_filter_res(blocks_of_int, CONTRACTS_BK, args)
        
        return blocks_of_int 
    

    def make_trace_filter_params(self, args, search_addr):
        if(isinstance(search_addr,list)):
            logger.info('applying trace filter using multiple addresses')

            search_list = search_addr
        else:
            search_list = list({search_addr})
        
        # setting param for the right position
        if(args.pos=='to'):
            pos_arg = "toAddress"
             
        elif(args.pos=='from'):
            pos_arg = "fromAddress"

        else:
            logger.error("invalid pos argument. Please check if the pos argument is one of \"to\" or \"from\"")
        return_val =  [
                {
                "fromBlock": f"{hex(int(args.start_block))}",
                "toBlock": f"{hex(int(args.end_block))}",
                pos_arg: search_list
                }
            ]
    
        return return_val
    # by heuristic, querying up until ~100 blocks is possible for the default server-side timeout (timeout=5s, Alchemy)
    def send_trace_filter_req(self, args, search_addr):
        url = f"{self.apis['RPC_PROVIDER']}"
        
        req={
                "id": 1,
                "jsonrpc": "2.0",
                "method": "trace_filter",
                "params": self.make_trace_filter_params(args, search_addr)
                  
                }

        response = self.send_request(url, req) 
        if response.status_code == 200:
            data = response.json()
            if ('result' in data and len(data['result']) > 0):
                length = len(data['result'])
                logger.info(f'fetched {length} traces')
                return data['result']
            
        
        logger.info(f'no traces were found for the search range : {args.start_block} - {args.end_block}')
        logger.error(f'response code : {response.status_code}')
        return None
        
    def format_traces(self, traces):
        collect = pd.DataFrame()
        for tr in traces:
            tx = {}

            # actions
            if('action' in list(tr.keys())):
                action = tr['action']
                for k,v in action.items():
                    tx[k] = v
                
            tx['blockHash'] = tr['blockHash']
            tx['blockNumber'] = tr['blockNumber']
            if(tr['result'] is None): # some edge case where trace itself is not none but 'result' is nonetype
                logger.error(f'spotted a failed execution skipping to the next trace: result field didn\'t exist')
                continue
            tx['gasUsed'] = tr['result']['gasUsed']
            if(not('output' in tr['result'])):
                logger.error(f'spotted a failed execution skipping to the next trace: output field didn\'t exist')
                continue
            tx['output'] = tr['result']['output']
            tx['subtraces']= tr['subtraces']
            tx['traceAddress']= tr['traceAddress']
            tx['transactionPosition'] = tr['transactionPosition']
            tx['type'] = tr['type']


            # try to decipher inputs 
            decoded, abi_exists = self.decoding_handler(tx['to'], tx['input'], tx['value'])

            tx['decoded'] = decoded
            tx['abi'] = abi_exists

            tx_df = pd.DataFrame([tx])
            collect = pd.concat([collect, tx_df])
        
        #reset index
        collect = collect.reset_index(drop=True)

        return collect

    def public_library_check(self, hex_input):
        # get first 8 hex digits (4 bytes) + 2 (0x)
        hex_signature = hex_input[:10] 
        text_signature = self.query_public_library(hex_signature)
        if(text_signature is None):
            logger.error(f'input hex : {hex_signature} was not found in the public library. Input cannot be decoded')
            decoded = hex_signature
        else: 
            # WIP for further decoding the input using the text_signature
            # parsed_dtypes = self.parse_dtypes(text_signature)
            # hex_data = bytes.fromhex(hex_input)
            # decodedABI = eth_abi.abi.decode(parsed_dtypes, hex_data)
            decoded = text_signature
        return decoded

    def decoding_handler(self, contract_addr, hex_input:str, trace_value):
        contract_addr = Web3.to_checksum_address(contract_addr) 

        # first check if ABI exists on Etherscan
        contract_abi, verdict = self.abi_handler_addr_pos(contract_addr, self.apis['ETHERSCAN_API'])
        
        # params (will be None except we have a matching ABI)
        abi_exists = 0
        # try to decode the input using ABI
        if(verdict=='contract'):
            if(len(hex_input)>2): #handling null and 0x
                if(contract_abi is None):
                    logger.info(f'fetching ABI failed. Trying query public byte library')
                    decoded = self.public_library_check(hex_input)
                
                else: # in case we have a matching ABI
                    try:
                        func, params = self.decode_input(hex_input, contract_addr, contract_abi)
                    except Exception as e:
                        print(e)
                        logger.error(f'suspecting a client problem (for decoding inputs using ABI). If the error was about \"insufficientDataBytes\" it could be a geth problem. https://github.com/ethereum/web3.py/issues/1257')
                        func = hex_input[:10] 
                        params = 'ABI_reading_problem'
                        decoded = self.public_library_check(hex_input)

                    
                    # return decoded input
                    if(type(params)!= dict): # abnormal decoded parameters
                        decoded = func
                    else:
                        decoded = func.function_identifier
                    abi_exists = 1 # here we deliberately ignore decoded params for better batch-level analysis downstream by just flagging existence of ABI.
            # if input is 0x, need to check a value. If value is also 0x then likely a fallback function and if value is not 0x then likely a unwrapping (ether transfer) is happening.
            else:

                if(len(trace_value)>2 and trace_value!='0x0'): # to be extra strict
                    eth_convert = int(trace_value, 16)/10**18
                    logger.info(f'likely a unwrapping event (ether transfer) {eth_convert:.3f} ETH')
                    decoded = 'ether transfer(contract)'    
                
                else:
                    logger.info(f'probably a fallback function of a contract(\'to\') getting called')
                    decoded = 'fallback'
            
        else:
            # for cases where it was an ether transfer or a contract creation

            logger.info(f'\'to\' address was not a contract')
            if(len(hex_input)>2):
                logger.info(f'possible contract creation')
                decoded = 'contract creation'
            else:
                logger.info(f'simple ether transfer')
                decoded = 'ether transfer(EOA)'


        return  decoded, abi_exists


        
    def _get_traces_filter(self, args, search_addr):
        # check if its in checksum address
        search_addr = Web3.to_checksum_address(search_addr) 
        # get traces
        traces = self.send_trace_filter_req(args, search_addr)
        if(traces is not None):
            logger.info(f'fomatting filter traces for the address : {search_addr}')
            # get target blocks
            formatted = self.format_traces(traces)
            return formatted
        else:
            # no trace was found.
            return None

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
    



    def get_traces_from_filter(self, CONTRACTS, args):
        for search_addr in CONTRACTS:
            logger.info(f'getting traces for the address : {search_addr}, starting block : {args.start_block} ending block : {args.end_block} at {args.pos} position')
            # check if its cached
            cache_root = f'./output/{self.DATE}/{args.start_block}_{args.end_block}/traces_{args.pos}'
            cache_file = f'{cache_root}/{search_addr}.csv'
            
            if(os.path.exists(cache_file)):
                logger.info(f'traces for the address : {search_addr} was already exported for the given block range')
                
            else:
                target_traces = self._get_traces_filter(args, search_addr)
                
                if(target_traces is not None): # make sure that traces were exported.
                    logger.info(f'found {len(target_traces)} trace entries for addr : {search_addr}')
                    utils.check_dir(cache_root)
                    target_traces.to_csv(cache_file, index=False)
                    logger.info(f'exporting done.')
                else:
                    logger.error(f'no logs were found for the given range of blocks')
    
    def get_all_traces(self, CONTRACTS, args):
        
        # check if its cached
        cache_root = f'./output/{self.DATE}/trace_out/{args.blocknumber}'
        cache_file = f'{cache_root}/traced_out.csv'
        
        if(os.path.exists(cache_file)):
                logger.info(f'traces for the block : {args.blocknumber} / tx_pos : {args.tx_pos} was already exported')
                
        else:
            target_traces = self._get_all_traces(CONTRACTS, args)
            
            if(target_traces is not None):
                logger.info(f'found {len(target_traces)} trace entries for tx_pos : {args.tx_pos}')
                utils.check_dir(cache_root)
                target_traces.to_csv(cache_file, index=False)
                logger.info(f'exporting done.')
            else:
                logger.error(f'no logs were found for the given range of blocks')

    def _get_all_traces(self, CONTRACTS, args):
        if(CONTRACTS != 'no_target'):
            logger.info(f'tracing out with target addresses')
        
        else:
            logger.info(f'exporting all traces for the given tx positions without using target addresses')
        
        
        # get traces
        traces = self.fetch_blocktrace(self.apis['RPC_PROVIDER'])
        
        # subset them (transactionPosition)
        subset = []
        for tr in traces:
            if('transactionPosition' in tr):
                if(str(tr['transactionPosition']) in args.tx_pos):
                    subset.append(tr)
            
       
       
        if(traces is not None):
            logger.info(f'fomatting exported traces...')
            # get target blocks
            formatted = self.format_traces(subset)
            return formatted
        else:
            # no trace was found.
            return None




#below for debugging purpose
'''
import argparse

filter_parser = argparse.ArgumentParser()
# traces by applying filters (2k range limit) 
filter_parser.add_argument("--start_block", "-sb", type=str, required=True, help="starting blocknumber")
filter_parser.add_argument("--end_block", '-eb', type=str, required=True, help="ending blocknumber")
filter_parser.add_argument("--addr", "-a", type=str, nargs='+', required=True,help="Contract address of interest")
filter_parser.add_argument("--pos", "-p", type=str, nargs='+', required=True,help="Contract address position")
filter_parser.add_argument("--job_id", "-j", type=str, default='0', help="job id for running multiple jobs")
args = argparse.Namespace(start_block='16308390',end_block = '16308489', addr=['0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'], pos = 'to', job_id= '0')


# export all traces given a blocknumber and a target transaction position 
filter_parser.add_argument("--blocknumber", "-b", type=str, required=True, help="target blocknumber")
filter_parser.add_argument("--tx_pos", "-p", type=str, nargs='+', required=True,help="target transaction position")
filter_parser.add_argument("--addr", "-a", type=str, nargs='+', default= 'no_target', required=False, help="Contract address(es) of interest (optional filter)")
filter_parser.add_argument("--job_id", "-j", type=str, default='0', help="job id for running multiple jobs")
args = argparse.Namespace(blocknumber='17930329', tx_pos=['3', '5'], addr= 'no_target', job_id= '0')




 -sb 17930229 -eb 17930328 -a 0xE4000004000bd8006e00720000d27d1FA000d43e -p from



search_addr = Web3.to_checksum_address(args.addr[0])
'''
    


     

