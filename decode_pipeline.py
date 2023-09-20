import os,sys
from Eth_ETL import Eth_tracker 
import logging
import pandas as pd
sys.path.append('./decode')
from traces_decoder import Transfer_Decoder
            
logger = logging.getLogger(__name__)


def run_job(args, w3, apis):
    

    # initialize custom class
    td = Transfer_Decoder(eth_tracker_loc='./', w3=w3, API = apis)



    if args.job=='decode_trace':
       
        logger.info(f"decoding exported traces with a search keyword {args.search_keyword}, decoding : {args.exported_file}")
        
        #initialize transfer decoder
        

        if(args.search_keyword == 'transfer'):
           
            result = td.decode_trace_csv(args.exported_file, args.search_keyword)
            # save it
            parsed= args.exported_file.split('/')
            parent_dir = '/'.join(parsed[:-1])
            result.to_csv(f'{parent_dir}/{args.search_keyword}.csv', index=False)
            logger.info(f'decoding job successfully finished for {parent_dir}/{args.search_keyword}.csv')

        else:
            logger.error(f"decoding for {args.search_keyword} not yet implemented")

    

    else:
        print("Please specify a valid job alias.")


