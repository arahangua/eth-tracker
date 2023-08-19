import os,sys
from Eth_ETL import Eth_tracker 
import logging
import pandas as pd


logger = logging.getLogger(__name__)

def run_job(args, w3, apis):
    # Config vars
    # BLOCK_ID = '17781200'
    # CONTRACTS = '0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D'
    

    BLOCK_ID = args.blocknumber#'17781200'

    # check if the addr input was a txt file
    if('.' in args.addr[0]):
        file_str= args.addr[0]
        parsed = file_str.split('.')
        logger.info(f'reading a separate file for contract addresses of interest : found a {parsed[-1]} file')
        CONTRACTS= handle_addr_file(file_str, parsed[-1])
    else:
        CONTRACTS = args.addr #'0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D' or a list of addresses
    ETHERSCAN_API = apis['ETHERSCAN_API'] 
    RPC_PROVIDER = apis['RPC_PROVIDER'] 



    # initialize custom class
    eth = Eth_tracker(w3, BLOCK_ID, CONTRACTS)

    if args.job == "contracts_from":
        logger.info(f"extracting contracts that gets called from the input contract {args.addr} in block {args.blocknumber}")
        # fetch blockinfo
        blockinfo = eth.fetch_blockinfo()
        # get transactions 
        txs = eth.get_transactions(blockinfo)
        # get contracts that were called 
        eth.find_interacting_addrs(txs, addr_pos='from')
        


    elif args.job == "contracts_to":
        logger.info(f"extracting contracts that make calls to the input contract {args.addr} in block {args.blocknumber}")
        # fetch blockinfo
        blockinfo = eth.fetch_blockinfo()
        # get transactions 
        txs = eth.get_transactions(blockinfo)
        # get contracts that were called 
        eth.find_interacting_addrs(txs, addr_pos='to')
        


    elif args.job == "txs_from":
        logger.info(f"extracting transactions that were called from the input contract {args.addr} in block {args.blocknumber}")
         # fetch blockinfo
        blockinfo = eth.fetch_blockinfo()
        # get transactions 
        txs = eth.get_transactions(blockinfo)
        # get contracts that were called 
        eth.find_interacting_contracts(txs, ETHERSCAN_API=ETHERSCAN_API, addr_pos='from')

    elif args.job == "txs_to":
        logger.info(f"extracting transactions that gets called from the input contract {args.addr} in block {args.blocknumber}")
        # fetch blockinfo
        blockinfo = eth.fetch_blockinfo()
        # get transactions 
        txs = eth.get_transactions(blockinfo)
        # get contracts that were called 
        eth.find_interacting_contracts(txs, ETHERSCAN_API=ETHERSCAN_API, addr_pos='to')

    elif args.job == "traces_from":
        logger.info(f"extracting traces of the transactions that were called by the input contract {args.addr} in block {args.blocknumber}")
         # fetch blockinfo
        blocktrace = eth.fetch_blocktrace(RPC_PROVIDER)
        # get transactions 
        actions = eth.get_trace_actions(blocktrace)
        # get contracts that were called 
        eth.find_interacting_traces(actions, blocktrace, ETHERSCAN_API=ETHERSCAN_API, addr_pos='from')


    elif args.job == "traces_to":
        logger.info(f"extracting traces of the trancsactions that called the input contract {args.addr} in block {args.blocknumber}")
         # fetch blockinfo
        blocktrace = eth.fetch_blocktrace(RPC_PROVIDER)
        # get transactions 
        actions = eth.get_trace_actions(blocktrace)
        # get contracts that were called 
        eth.find_interacting_traces(actions, blocktrace, ETHERSCAN_API=ETHERSCAN_API, addr_pos='to')


    else:
        print("Please specify a valid job alias.")
       
def handle_addr_file(file_loc, ext):
    if(ext=='txt'):
        return read_txt(file_loc)
    elif(ext=='csv'):
        return read_csv(file_loc)
    else:
        logger.error(f"extension: {ext} is not yet implemented for reading-in contract addresses")

def read_txt(file_loc):
    with open(file_loc, 'r') as read_file:    
        lines = read_file.readlines()

    # Remove any trailing newline characters from each line
    lines = [line.strip() for line in lines]
    return lines 


def read_csv(file_loc):
    logger.debug(f'for csv files, the script looks for \'address\' column by default')
    csv_file = pd.read_csv(file_loc)
    return csv_file['address'].tolist()
