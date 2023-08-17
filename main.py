import os, sys
import logging
from dotenv import load_dotenv
from web3 import Web3
import pandas as pd
import etl_pipeline
from arg_parser import get_args

#loading API key
load_dotenv()
apis = {}
apis['ETHERSCAN_API'] = os.getenv('ETHERSCAN_API')
apis['RPC_PROVIDER'] = os.getenv('ETH_MAINNET_EXECUTION_RPC') # change this part for other EVM compatible rpc endpoints (*make sure if the downstream application is compatible regarding Etherscan side)


#configure w3 connection
w3 = Web3(Web3.HTTPProvider(apis['RPC_PROVIDER']))
assert w3.is_connected(), 'please check rpc provider configuration, web3 connection is not established'


# Configure logging
logging.basicConfig(filename='eth_etl.log', level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', filemode='w')
# for terminal outputs
sh = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
sh.setFormatter(formatter)
sh.setLevel(logging.INFO)
logging.getLogger().addHandler(sh)

logger = logging.getLogger(__name__)
# parseing arguments
args = get_args()

# run the job 
etl_pipeline.run_job(args = args, w3=w3, apis = apis)

logger.info("job succesfully done, please check eth_etl.log for the job log")
