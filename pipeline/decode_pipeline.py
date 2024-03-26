import os,sys
from Eth_ETL import Eth_tracker 
import logging
import pandas as pd
sys.path.append('./decode')
from traces_decoder import Transfer_Decoder
            
logger = logging.getLogger(__name__)


def run_job(args, w3, apis, DATE):
    

    # initialize custom class
    td = Transfer_Decoder(eth_tracker_loc='./', w3=w3, API = apis, DATE=DATE)
    if hasattr(args, 'transfer_func_patterns'):
        if(args.transfer_func_patterns=='False'): #str to bool handling
            args.transfer_func_patterns = False
        else:
            args.transfer_func_patterns = True




    if args.job=='decode_trace':
       
        logger.info(f"decoding exported traces with a search keyword {args.search_keyword}, decoding : {args.exported_file}")
        
        # fetching the parent dir
        parsed= args.exported_file.split('/')
        parent_dir = '/'.join(parsed[:-1])


        if(args.search_keyword == 'transfer'):
            logger.info(f'use_known_pattern = {args.transfer_func_patterns}')
            result = td.decode_trace_csv(args.exported_file,  parent_dir, args.search_keyword, use_known_pattern = args.transfer_func_patterns)
            # save it
            parsed= args.exported_file.split('/')
            parent_dir = '/'.join(parsed[:-1])
            result.to_csv(f'{parent_dir}/{args.search_keyword}.csv', index=False)
            logger.info(f'decoding job successfully finished for {parent_dir}/{args.search_keyword}.csv')

        else:
            logger.error(f"decoding for {args.search_keyword} not yet implemented")

    elif args.job=='decode_logs':
       
        logger.info(f"decoding exported logs, decoding : {args.exported_file}")
        
        # fetching the parent dir
        parsed= args.exported_file.split('/')
        parent_dir = '/'.join(parsed[:-1])
        file_name = parsed[-1] # includes .csv
        #initialize transfer decoder
        

        result = td.decode_logs_csv(args.exported_file)
        # save it
        parsed= args.exported_file.split('/')
        parent_dir = '/'.join(parsed[:-1])
        result.to_csv(f'{parent_dir}/decoded_{file_name}', index=False)
        logger.info(f'decoding job successfully finished for {parent_dir}/decoded_{file_name}')

        
    else:
        print("Please specify a valid job alias.")


