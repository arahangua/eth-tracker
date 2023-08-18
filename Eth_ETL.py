import os, sys
import json
import requests
import logging
import pandas as pd
import hexbytes
import utils
import datetime 
from web3 import Web3

now = datetime.datetime.now()
# Format the date to 'day-month-year'
DATE = now.strftime('%d%m%y')


logger = logging.getLogger(__name__)

class Eth_tracker():
    def __init__(self, web3_instance, block_id, contract):
        self.w3 = web3_instance
        try:
            block_id = int(block_id)
        except:
            pass
        self.block_id = block_id
        self.contract = contract
        logger.info('eth etl class initialized')

    def fetch_blockinfo(self):

        blockinfo = self.w3.eth.get_block(self.block_id)
        logger.info(f'block fetched, block id: {self.block_id}')
        
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
            for key in collect_keys:
                if(key=='hash'):
                    summary[key]=tx[key].hex()
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
        search_entry = self.contract
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
        search_entry = self.contract
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
        search_entry = self.contract
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
        utils.check_dir(f"./output/{DATE}")
        subset.to_csv(f"./output/{DATE}/block_id_{self.block_id}_{addr_pos}_contract_addr_{search_entry}.csv", index=False)
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