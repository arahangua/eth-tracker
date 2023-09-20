# %reload_ext autoreload
# %autoreload 2
import os, sys
import logging
from dotenv import load_dotenv
from web3 import Web3
import pandas as pd
import etl_pipeline, decode_pipeline
from arg_parser import get_args

#loading API key
load_dotenv()
apis = {}
apis['ETHERSCAN_API'] = os.getenv('ETHERSCAN_API')
apis['RPC_PROVIDER'] = os.getenv('ETH_MAINNET_EXECUTION_RPC') # change this part for other EVM compatible rpc endpoints (*make sure if the downstream application is compatible regarding Etherscan side)
apis['PUBLIC_LIBRARY'] = os.getenv('PUBLIC_LIBRARY')

#configure w3 connection
w3 = Web3(Web3.HTTPProvider(apis['RPC_PROVIDER']))
assert w3.is_connected(), 'please check rpc provider configuration, web3 connection is not established'

# parsing arguments
args = get_args()


# Configure logging
if(not(os.path.exists('./logs'))):
    os.makedirs('./logs')
logging.basicConfig(filename=f'./logs/eth_etl_{args.job_id}.log', level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', filemode='w')
# for terminal outputs
sh = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
sh.setFormatter(formatter)
sh.setLevel(logging.INFO)
logging.getLogger().addHandler(sh)

logger = logging.getLogger(__name__)

# run the job 
decode_jobs = ['decode_trace']
if(args.job in decode_jobs):
    decode_pipeline.run_job(args = args, w3=w3, apis = apis)
else:
    etl_pipeline.run_job(args = args, w3=w3, apis = apis)

logger.info(f"job succesfully done, please check eth_etl_{args.job_id}.log for the job log")
