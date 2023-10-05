import argparse

def get_args():
    # set/parse cli arguments
    parser = argparse.ArgumentParser(description="eth ETL program")
    subparsers = parser.add_subparsers(dest="job", help="Choose a job to execute.")
        
    # Export interacting contract addresses given the blocknumber and the "from" contract.
    cf_parser = subparsers.add_parser("contracts_from", help="export contracts that are called from the input address")
    cf_parser.add_argument("--blocknumber", "-b",  type=str, default='latest', help="Block number of interest")
    cf_parser.add_argument("--addr", "-a", type=str, nargs='+', required=True, help="Contract address of interest (from)")
    cf_parser.add_argument("--job_id", "-j", type=str, default='0', help="job id for running multiple jobs")

    # Export interacting contract addresses given the blocknumber and the "to" contract.
    ct_parser = subparsers.add_parser("contracts_to", help="export contracts that make calls to the input address (for inter-contract calls, use traces_* jobs)")
    ct_parser.add_argument("--blocknumber", "-b", type=str, default='latest', help="Block number of interest")
    ct_parser.add_argument("--addr", "-a",type=str, nargs='+',required=True, help="Contract address of interest (to)")
    ct_parser.add_argument("--job_id", "-j", type=str, default='0', help="job id for running multiple jobs")

    # Export transactions given the blocknumber and the "from" contract.
    txf_parser = subparsers.add_parser("txs_from", help="export transactions that were from the input address ")
    txf_parser.add_argument("--blocknumber","-b", type=str, default='latest', help="Block number of interest")
    txf_parser.add_argument("--addr", "-a",type=str, nargs='+',required=True, help="Contract address of interest (from)")
    txf_parser.add_argument("--job_id", "-j", type=str, default='0', help="job id for running multiple jobs")


    # Export transactions given the blocknumber and the "to" contract.
    txt_parser = subparsers.add_parser("txs_to", help="export transactions that were making calls to the input address (for inter-contract calls, use traces_* jobs)")
    txt_parser.add_argument("--blocknumber", "-b",type=str, default='latest', help="Block number of interest")
    txt_parser.add_argument("--addr", "-a",type=str, nargs='+',required=True, help="Contract address of interest (to)")
    txt_parser.add_argument("--job_id", "-j", type=str, default='0', help="job id for running multiple jobs")

    # Export traces given the blocknnumber and the "from" contract. 
    trf_parser = subparsers.add_parser("traces_from", help="export traces of transactions that were from the input address")
    trf_parser.add_argument("--blocknumber", "-b",type=str, default='latest', help="Block number of interest")
    trf_parser.add_argument("--addr", "-a",type=str, nargs='+',required=True, help="Contract address of interest (from)")
    trf_parser.add_argument("--job_id", "-j", type=str, default='0', help="job id for running multiple jobs")



    # Export traces given the blocknnumber and the "to" contract. 
    trt_parser = subparsers.add_parser("traces_to", help="export traces of the transactions that were making calls to the input address")
    trt_parser.add_argument("--blocknumber", "-b",type=str, default='latest', help="Block number of interest")
    trt_parser.add_argument("--addr", "-a",type=str, nargs='+',required=True, help="Contract address of interest (to)")
    trt_parser.add_argument("--job_id", "-j", type=str, default='0', help="job id for running multiple jobs")


       
    # apply filter (if supported by the rpc provider) to get addrs/transactions/traces 
    filter_parser = subparsers.add_parser("apply_filter", help="apply filter on the range of blocknumbers to query addrs/transactions/traces")
    filter_parser.add_argument("--job_name", "-jn", type=str, required=True, help="job to run for each match in the filter. check help (-h) instructions")
    filter_parser.add_argument("--start_block", "-sb", type=str, required=True, help="starting blocknumber")
    filter_parser.add_argument("--end_block", '-eb', type=str, required=True, help="ending blocknumber")
    filter_parser.add_argument("--addr", "-a", type=str, nargs='+', required=True,help="Contract address of interest")
    filter_parser.add_argument("--job_id", "-j", type=str, default='0', help="job id for running multiple jobs")
    filter_parser.add_argument("--save_blocklist", "-sbl", type=bool, default=True, help="flag for saving interim block list")
    

    # get logs (2k range limit) 
    filter_parser = subparsers.add_parser("get_logs", help="apply filter on the range of blocknumbers to get event logs")
    filter_parser.add_argument("--start_block", "-sb", type=str, required=True, help="starting blocknumber")
    filter_parser.add_argument("--end_block", '-eb', type=str, required=True, help="ending blocknumber")
    filter_parser.add_argument("--addr", "-a", type=str, nargs='+', required=True,help="Contract address of interest")
    filter_parser.add_argument("--job_id", "-j", type=str, default='0', help="job id for running multiple jobs")
    
    # traces by applying filters (2k range limit) 
    filter_parser = subparsers.add_parser("trace_filter", help="apply trace filter on the range of blocknumbers to get traces")
    filter_parser.add_argument("--start_block", "-sb", type=str, required=True, help="starting blocknumber")
    filter_parser.add_argument("--end_block", '-eb', type=str, required=True, help="ending blocknumber")
    filter_parser.add_argument("--addr", "-a", type=str, nargs='+', required=True,help="Contract address of interest")
    filter_parser.add_argument("--pos", "-p", type=str, required=True,help="Contract address position")
    filter_parser.add_argument("--job_id", "-j", type=str, default='0', help="job id for running multiple jobs")
    
    # export all traces given a blocknumber and a target transaction position 
    filter_parser = subparsers.add_parser("trace_out", help="full export of a trace given a blocknumber and transaction position")
    filter_parser.add_argument("--blocknumber", "-b", type=str, required=True, help="target blocknumber")
    filter_parser.add_argument("--tx_pos", "-p", type=str, nargs='+', required=True,help="target transaction position")
    filter_parser.add_argument("--job_id", "-j", type=str, default='0', help="job id for running multiple jobs")
    



    ### decoding ###    

    # after trace_out job was executed, decode exported inputs with a specific search keyword for function names 
    filter_parser = subparsers.add_parser("decode_trace", help="decode exported inputs (from trace_out job) with a specific search keyword (function name)")
    filter_parser.add_argument("--search_keyword", "-s", type=str, required=True, help="search keyword for function names")
    filter_parser.add_argument("--exported_file", "-e", type=str, required=True,help="exported traces (csv file)")
    filter_parser.add_argument("--job_id", "-j", type=str, default='0', help="job id for running multiple jobs")
    
    ### price fetching ###

    filter_parser = subparsers.add_parser("price_current", help="fetch the most recent token price")
    filter_parser.add_argument("--source", "-s", type=str, required=True, help="source for the price (e.g., defillama, coingecko)")
    filter_parser.add_argument("--token", "-t", type=str, nargs = '+', required=True,help="token address(es) or path to a csv file")
    filter_parser.add_argument("--job_id", "-j", type=str, default='0', help="job id when running multiple jobs")

    # wip
    # filter_parser = subparsers.add_parser("price_historical", help="fetch price time series data")
    # filter_parser.add_argument("--source", "-s", type=str, required=True, help="source for the price (e.g., defillama, coingecko)")
    # filter_parser.add_argument("--token", "-t", type=str, nargs = '+', required=True,help="token address(es) or path to a csv file")
    # filter_parser.add_argument("--start_block", "-sb", type=str, required=True,help="start block")
    # filter_parser.add_argument("--end_block", "-eb", type=str, required=True,help="last block")
    # filter_parser.add_argument("--interval", "-i", type=int, required=False, default = 1, help="interval for fetch prices (in days, defaults to 1 day)")
    # filter_parser.add_argument("--job_id", "-j", type=str, default='0', help="job id when running multiple jobs")
   
    return parser.parse_args()


# fetch the right pipeline for the given job name
def job_parser(args):
    etl_jobs = ['contracts_from', 'contracts_to', 'txs_to', 'traces_to', 'txs_from', 'traces_from', 'apply_filter', 'get_logs', 'trace_filter', 'trace_out']
    decode_jobs = ['decode_trace']
    price_fetch_jobs = ['price_current', 'price_historical']

    if(args.job in etl_jobs):
        pipeline = 'eth_etl'
    elif(args.job in decode_jobs):
        pipeline = 'decode'
    elif(args.job in price_fetch_jobs):
        pipeline = 'price_fetch'
    else:
        raise ValueError(f'job alias : {args.job} not recognized by the parser')    

    return pipeline

# debugging 
"""
import argparse

filter_parser = argparse.ArgumentParser()
# traces by applying filters (2k range limit) 
filter_parser.add_argument("--source", "-s", type=str, required=True, help="source for the price (e.g., defillama, coingecko)")
filter_parser.add_argument("--token", "-t", type=str, nargs = '+', required=True,help="token address(es) or path to a csv file")
filter_parser.add_argument("--job_id", "-j", type=str, default='0', help="job id when running multiple jobs")
args = argparse.Namespace(job= 'price_current', source= 'defillama', token = '/home/takim/work/blockchain-graph-analytics/intermediate_results/transfer_balance/230923/unique_tokens.csv',  job_id= '0')




"""