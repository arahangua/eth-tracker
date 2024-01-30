# %reload_ext autoreload
# %autoreload 2
import os, sys
import logging
from dotenv import load_dotenv
from web3 import Web3
import pandas as pd
sys.path.append('./pipeline')
import etl_pipeline, decode_pipeline, price_fetch_pipeline
import arg_parser 
import datetime

# set datetime 
now = datetime.datetime.now()
# Format the date to 'day-month-year'
DATE = now.strftime('%d%m%y')



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
args = arg_parser.get_args()


# Configure logging
if(not(os.path.exists('./logs'))):
    os.makedirs('./logs')
logging.basicConfig(filename=f'./logs/job_{args.job_id}.log', level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', filemode='w')
# for terminal outputs
sh = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
sh.setFormatter(formatter)
sh.setLevel(logging.INFO)
logging.getLogger().addHandler(sh)

logger = logging.getLogger(__name__)

# run the job 
pipeline = arg_parser.job_parser(args)

if(pipeline == 'eth_etl'):
    etl_pipeline.run_job(args = args, w3=w3, apis = apis, DATE=DATE)
elif(pipeline == 'decode'):
    decode_pipeline.run_job(args = args, w3=w3, apis = apis, DATE=DATE)
elif(pipeline == 'price_fetch'):
    price_fetch_pipeline.run_job(args = args, w3=w3, apis = apis, DATE=DATE)

logger.info(f"job succesfully done, please check job_{args.job_id}.log for the job log")
