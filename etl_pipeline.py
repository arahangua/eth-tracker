import os,sys
from Eth_ETL import Eth_tracker 
import logging


logger = logging.getLogger(__name__)

def run_job(args, w3, apis):
    # Config vars
    BLOCK_ID = args.blocknumber#'17781200'
    CONTRACTS = args.addr #'0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D'
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
        logger.info(f"extracting contracts that gets called from the input contract {args.addr} in block {args.blocknumber}")
        

    elif args.job == "txs_to":
        logger.info(f"extracting contracts that gets called from the input contract {args.addr} in block {args.blocknumber}")
        

    elif args.job == "traces_from":
        logger.info(f"extracting contracts that gets called from the input contract {args.addr} in block {args.blocknumber}")
        

    elif args.job == "traces_to":
        logger.info(f"extracting contracts that gets called from the input contract {args.addr} in block {args.blocknumber}")
            
    else:
        print("Please specify a valid mode.")
       
