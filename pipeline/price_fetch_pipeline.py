import os,sys
from Eth_ETL import Eth_tracker 
import logging
import pandas as pd
sys.path.append('./price')
import price_fetch
            
logger = logging.getLogger(__name__)


def run_job(args, w3, apis, DATE):
    
    
    #initialize the class for price retrieval
    if args.source == 'defillama':
        logger.info(f"fetching token price from defillama")
        pf = price_fetch.Defillama(eth_tracker_loc='./', w3 = w3, API=apis, DATE=DATE)
    elif args.source == 'coingecko':
        logger.info(f"fetching token price from coingecko")
        pf = price_fetch.Coingecko()
    else:
        logger.error(f'price source : {args.source} not implemented.')    
        raise ValueError(f'unrecogized price source : {args.source}')



    if args.job=='price_current':
       
        logger.info(f"getting the most most recent token price (https://defillama.com/docs/api)")
        if('csv' in args.token[0]):
            logger.info(f'found a csv file as an input to \'token\' input proceeding in batch processing mode.')
            pf.get_current_price_batch(args)
        else:
            pf.get_current_price(args)
    

        
        

            
    



