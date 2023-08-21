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

now = datetime.datetime.now()
# Format the date to 'day-month-year'
DATE = now.strftime('%d%m%y')


logger = logging.getLogger(__name__)

class Eth_tracker():
    def __init__(self, web3_instance, block_id, contracts):
        self.w3 = web3_instance
        try:
            block_id = int(block_id)
        except:
            pass
        self.block_id = block_id
        self.contracts = contracts
        logger.info('eth etl class initialized')

    def fetch_blockinfo(self):

        blockinfo = self.w3.eth.get_block(self.block_id)
        logger.info(f'block fetched, block id: {self.block_id}')
        # print(blockinfo)
        
        return blockinfo
        
    def fetch_blocktrace(self, rpc_provider):
        
        url = f"{rpc_provider}"
        
        request={
                    "jsonrpc":"2.0",
                    "method":"trace_block",
                    "params":[str(self.block_id)],
                    "id":1
                }
        
        response = requests.post(url, json=request)
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






    def get_contract_abi(self, addr:str, ETHERSCAN_API=None):
        code = self.w3.eth.get_code(addr)
        # Check if the address is a contract account
        if code.hex() == '0x':
            print(f"{addr} is an EOA")
            return None, 'eoa'
        else:
            print(f"{addr} is a contract. Fetching abi...")
            contract_abi = self.get_abi(addr, ETHERSCAN_API)
            return contract_abi, 'contract'
        
    def decode_input(self, input, contract_addr, contract_abi):
        contract = self.w3.eth.contract(address=contract_addr, abi=contract_abi)
        func, params = contract.decode_function_input(input)

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
            inner['decoded']['function_name']=func.function_identifier
            inner['decoded']['values']=params
            write['output']=inner
        
        utils.check_dir(f"./output/{DATE}/tx/")
        with open(f"./output/{DATE}/tx/block_id_{self.block_id}_{addr_pos}_contract_addr_{search_entry}_tx_{inner['transaction_hash']}.txt", 'w') as outfile:
            json.dump(write, outfile)
        logger.info(f"results (contracts that were calling the target contract) for {self.block_id} and the contract {search_entry} was successfully saved")

    def write_result_addrs(self, search_entry, subset, addr_pos):
        utils.check_dir(f"./output/{DATE}/addrs")
        subset.to_csv(f"./output/{DATE}/addrs/block_id_{self.block_id}_{addr_pos}_contract_addr_{search_entry}.csv", index=False)
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
                    inner['decoded']['output']['function_name']=func.function_identifier
                    inner['decoded']['output']['values']=params
                
                except Exception as e:
                    logger.error(f"output could not be decoded. output: {output}  \n tx_hash: {inner['transactionHash']}, skipping the decoding of the output. Error raised : {e}")
                
            write['trace']=inner
        utils.check_dir(f"./output/{DATE}/traces")
        with open(f"./output/{DATE}/traces/block_id_{self.block_id}_{addr_pos}_contract_addr_{search_entry}_trace_index_{action_subset.name}.txt", 'w') as outfile:
            json.dump(write, outfile)
        logger.info(f"results (traces that were calling the target contract) for {self.block_id} and the contract {search_entry} was successfully saved")

    


    def get_abi(self, contract_addr:str, ETHERSCAN_API=None, contract_type=None):
        # check its its cached/called before
        cached_file = f"./abis/{contract_addr}.txt"
        if os.path.exists(cached_file):
            with open(cached_file, 'r') as infile:
                abi_result = json.load(infile)
                print("using cached abi")
            return abi_result
        
        url = f"https://api.etherscan.io/api?module=contract&action=getabi&address={contract_addr}&apikey={ETHERSCAN_API}"
        response = requests.get(url)
        res = response.json()


        if res['status'] == '1':
            utils.check_dir(f"./abis")
            with open(cached_file, 'w') as outfile:
                json.dump(res['result'], outfile)
                print('abi saved')

            return str(res['result'])
            
        else:
            print(f"Error: {res['message']} Result: {res['result']}")    
            return # contract cannot be initialized with abi (could use function signatures for targeted approach)
    
    
    
    def abi_handler_addr_pos(self, search_addr, ETHERSCAN_API=None):
            # check if the interacting address is a contract
        search_addr = Web3.to_checksum_address(search_addr)
        contract_abi, verdict = self.get_contract_abi(search_addr, ETHERSCAN_API)
        if(contract_abi is None):
            logger.error(f"ABI for the contract {search_addr} is not recoverable. Skipping the decoding step")
        else:
            logger.info(f"ABI for the contract {search_addr} succefully fetched.")
        
                
        return contract_abi, verdict
    

    def get_blocks_filter(self, args, search_addr):
        # check if its in checksum address
        search_addr = Web3.to_checksum_address(search_addr) 
        filter_params = self.make_filter(args, search_addr)
        #get logs
        logs = self.w3.eth.get_logs(filter_params)
        if(len(logs)>0):
            logger.info(f'got filter logs for the address : {search_addr}')
            # get target blocks
            target_blocks = self.extract_blocks_from_logs(logs)
            return target_blocks
        else:
            logger.error(f'returned logs were empty')
            return None
        
    def get_logs_filter(self, args, search_addr):
        # check if its in checksum address
        search_addr = Web3.to_checksum_address(search_addr) 
        filter_params = self.make_filter(args, search_addr)
        #get logs
        logs = self.w3.eth.get_logs(filter_params)
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
            cache_root = f'./output/{args.start_block}_{args.end_block}/logs'
            cache_file = f'{cache_root}/{search_addr}.csv'
            
            if(os.path.exists(cache_file)):
                logger.info(f'logs for the address : {search_addr} was already exported for the given block range')
                
            else:
                target_logs = self.get_logs_filter(args, search_addr)
                
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

#below for debugging purpose
'''
import argparse
filter_parser = argparse.ArgumentParser()
filter_parser.add_argument("--job_name", "-jn", type=str, required=True, help="job to run for each match in the filter. check help (-h) instructions")
filter_parser.add_argument("--start_block", "-sb", type=str, required=True, help="starting blocknumber")
filter_parser.add_argument("--end_block", '-eb', type=str, required=True, help="ending blocknumber")
filter_parser.add_argument("--addr", "-a", type=str, nargs='+', required=True,help="Contract address of interest")
filter_parser.add_argument("--job_id", "-j", type=str, default='0', help="job id for running multiple jobs")

# Manually create the args variable
args = argparse.Namespace(job_name='contracts_to', start_block='17594916',end_block = '17595016', addr=['0x5a98fcbea516cf06857215779fd812ca3bef1b32'], job_id='0')

search_addr = Web3.to_checksum_address(args.addr[0])
'''
    


     

