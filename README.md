# eth-tracker

**1. setup .env file**

```
ETHERSCAN_API = <your etherscan api key>
ETH_MAINNET_EXECUTION_RPC = <your rpc provider>
```


**2. run command to see implemented functions**

```
python main.py -h
```
example output:

```
usage: main.py [-h] {contracts_from,contracts_to,txs_to,traces_to,txs_from,traces_from,apply_filter,get_logs} ...

eth ETL program

positional arguments:
  {contracts_from,contracts_to,txs_to,traces_to,txs_from,traces_from,apply_filter,get_logs}
                        Choose a job to execute.
    contracts_from      export contracts that are called from the input contract
    contracts_to        export contracts that make calls to the input contract
    txs_to              export transactions that were making calls to the input contract
    traces_to           export traces of the transactions that were making calls to the input contract
    txs_from            export transactions that were from the input contract
    traces_from         export traces of transactions that were from the input contract
    apply_filter        apply filter on the range of blocknumber to query addrs/transactions/traces
    get_logs            apply filter on the range of blocknumber to get event logs

options:
  -h, --help            show this help message and exit
```

**3. Batch jobs / multiprocessing**

Create "batch_jobs.txt" with each line corresponding to ETL job (see above)(check subprocess_handler.py)
Open subproecss_handler.py and change job parameters (e.g., set process_n = <number of process>)
then run
```
python subprocess_handler.py
```
