import argparse

def get_args():
    # set/parse cli arguments
    parser = argparse.ArgumentParser(description="eth ETL program")
    subparsers = parser.add_subparsers(dest="job", help="Choose a job to execute.")
        
    # Export interacting contract addresses given the blocknumber and the "from" contract.
    cf_parser = subparsers.add_parser("contracts_from", help="export contracts that are called from the input contract")
    cf_parser.add_argument("--blocknumber", "-b",  type=str, default='latest', help="Block number of interest")
    cf_parser.add_argument("--addr", "-a", type=str, nargs='+', required=True, help="Contract address of interest (from)")

    # Export interacting contract addresses given the blocknumber and the "to" contract.
    ct_parser = subparsers.add_parser("contracts_to", help="export contracts that make calls to the input contract")
    ct_parser.add_argument("--blocknumber", "-b", type=str, default='latest', help="Block number of interest")
    ct_parser.add_argument("--addr", "-a",type=str, nargs='+',required=True, help="Contract address of interest (to)")


    # Export transactions given the blocknumber and the "to" contract.
    txt_parser = subparsers.add_parser("txs_to", help="export transactions that were making calls to the input contract")
    txt_parser.add_argument("--blocknumber", "-b",type=str, default='latest', help="Block number of interest")
    txt_parser.add_argument("--addr", "-a",type=str, nargs='+',required=True, help="Contract address of interest (to)")

    # Export traces given the blocknnumber and the "to" contract. 
    trt_parser = subparsers.add_parser("traces_to", help="export traces of the transactions that were making calls to the input contract")
    trt_parser.add_argument("--blocknumber", "-b",type=str, default='latest', help="Block number of interest")
    trt_parser.add_argument("--addr", "-a",type=str, nargs='+',required=True, help="Contract address of interest (to)")


    # Export transactions given the blocknumber and the "from" contract.
    txf_parser = subparsers.add_parser("txs_from", help="export transactions that were from the input contract")
    txf_parser.add_argument("--blocknumber","-b", type=str, default='latest', help="Block number of interest")
    txf_parser.add_argument("--addr", "-a",type=str, nargs='+',required=True, help="Contract address of interest (from)")

    # Export traces given the blocknnumber and the "from" contract. 
    trf_parser = subparsers.add_parser("traces_from", help="export traces of transactions that were from the input contract")
    trf_parser.add_argument("--blocknumber", "-b",type=str, default='latest', help="Block number of interest")
    trf_parser.add_argument("--addr", "-a",type=str, nargs='+',required=True, help="Contract address of interest (from)")


    return parser.parse_args()


