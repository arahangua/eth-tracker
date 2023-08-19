import os,sys
from Eth_ETL import Eth_tracker 
import logging


logger = logging.getLogger(__name__)

def run_job(args, w3, apis):
    # Config vars
    # BLOCK_ID = '17781200'
    # CONTRACTS = '0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D'
    

    BLOCK_ID = args.blocknumber#'17781200'
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
       
