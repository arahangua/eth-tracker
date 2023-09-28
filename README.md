# eth-tracker

![two-hop neighborhood of a target address](./src/by_txtype_with_legend.png)
*Gephi-visualized example trace graph*


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
usage: main.py [-h] {contracts_from,contracts_to,txs_to,traces_to,txs_from,traces_from,apply_filter,get_logs,trace_filter,trace_out,decode_trace,price_current} ...

eth ETL program

positional arguments:
  {contracts_from,contracts_to,txs_to,traces_to,txs_from,traces_from,apply_filter,get_logs,trace_filter,trace_out,decode_trace,price_current}
                        Choose a job to execute.
    contracts_from      export contracts that are called from the input contract
    contracts_to        export contracts that make calls to the input contract
    txs_to              export transactions that were making calls to the input contract
    traces_to           export traces of the transactions that were making calls to the input contract
    txs_from            export transactions that were from the input contract
    traces_from         export traces of transactions that were from the input contract
    apply_filter        apply filter on the range of blocknumbers to query addrs/transactions/traces
    get_logs            apply filter on the range of blocknumbers to get event logs
    trace_filter        apply trace filter on the range of blocknumbers to get traces
    trace_out           full export of a trace given a blocknumber and transaction position
    decode_trace        decode exported inputs (trace_out) with a specific search keyword for function names
    price_current       fetch the most recent token price

options:
  -h, --help            show this help message and exit
```

**3. Batch jobs / multiprocessing**

Create "batch_jobs.txt" with each line corresponding to ETL job (see above)(check subprocess_handler.py) <br>
Open subproecss_handler.py and change job parameters (e.g., set process_n = 30) <br>
then run
```
python subprocess_handler.py -b <your batch script>
```

**4. Notes on Decoding Inputs (tx/trace)**

All workflows, with the exception of the "trace_filter" job, attempt to fetch ABIs from Etherscan for suitable input data that can be decoded. If the ABI is unavailable from Etherscan:

- The decoding step is skipped.
- The input data is not exported.

For the "trace_filter" and "trace_out" jobs:

- It always exports the raw input.<br>
- It aims to export at least the function identifier by:<br>
1. Checking Etherscan for ABIs.<br>
2. If the above fails, it checks the Ethereum public byte library for the function signature. <br>
3. If all attempts fail, it still exports the hex signature of the function extracted from the input data.