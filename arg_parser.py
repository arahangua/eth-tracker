import argparse

def get_args():
    # set/parse cli arguments
    parser = argparse.ArgumentParser(description="eth ETL program")
    subparsers = parser.add_subparsers(dest="job", help="Choose a job to execute.")
        
    # Export interacting contract addresses given the blocknumber and the "from" contract.
    cf_parser = subparsers.add_parser("contracts_from", help="export contracts that are called from the input contract")
    cf_parser.add_argument("--blocknumber", "-b",  type=str, default='latest', help="Block number of interest")
    cf_parser.add_argument("--addr", "-a", type=str, nargs='+', required=True, help="Contract address of interest (from)")
    cf_parser.add_argument("--job_id", "-j", type=str, default='0', help="job id for running multiple jobs")

    # Export interacting contract addresses given the blocknumber and the "to" contract.
    ct_parser = subparsers.add_parser("contracts_to", help="export contracts that make calls to the input contract")
    ct_parser.add_argument("--blocknumber", "-b", type=str, default='latest', help="Block number of interest")
    ct_parser.add_argument("--addr", "-a",type=str, nargs='+',required=True, help="Contract address of interest (to)")
    ct_parser.add_argument("--job_id", "-j", type=str, default='0', help="job id for running multiple jobs")


    # Export transactions given the blocknumber and the "to" contract.
    txt_parser = subparsers.add_parser("txs_to", help="export transactions that were making calls to the input contract")
    txt_parser.add_argument("--blocknumber", "-b",type=str, default='latest', help="Block number of interest")
    txt_parser.add_argument("--addr", "-a",type=str, nargs='+',required=True, help="Contract address of interest (to)")
    txt_parser.add_argument("--job_id", "-j", type=str, default='0', help="job id for running multiple jobs")

    # Export traces given the blocknnumber and the "to" contract. 
    trt_parser = subparsers.add_parser("traces_to", help="export traces of the transactions that were making calls to the input contract")
    trt_parser.add_argument("--blocknumber", "-b",type=str, default='latest', help="Block number of interest")
    trt_parser.add_argument("--addr", "-a",type=str, nargs='+',required=True, help="Contract address of interest (to)")
    trt_parser.add_argument("--job_id", "-j", type=str, default='0', help="job id for running multiple jobs")


    # Export transactions given the blocknumber and the "from" contract.
    txf_parser = subparsers.add_parser("txs_from", help="export transactions that were from the input contract")
    txf_parser.add_argument("--blocknumber","-b", type=str, default='latest', help="Block number of interest")
    txf_parser.add_argument("--addr", "-a",type=str, nargs='+',required=True, help="Contract address of interest (from)")
    txf_parser.add_argument("--job_id", "-j", type=str, default='0', help="job id for running multiple jobs")

    # Export traces given the blocknnumber and the "from" contract. 
    trf_parser = subparsers.add_parser("traces_from", help="export traces of transactions that were from the input contract")
    trf_parser.add_argument("--blocknumber", "-b",type=str, default='latest', help="Block number of interest")
    trf_parser.add_argument("--addr", "-a",type=str, nargs='+',required=True, help="Contract address of interest (from)")
    trf_parser.add_argument("--job_id", "-j", type=str, default='0', help="job id for running multiple jobs")





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
    
      
    
    
    
    return parser.parse_args()


