import os,sys
import subprocess
import numpy as np
import argparse 
import concurrent.futures


parser = argparse.ArgumentParser(description="batch job parser for eth-tracker")
parser.add_argument("--batch_script", "-b",  type=str, required=True, help="name of the batch script. Each line in the script should be one of eth-tracker jobs")
parser.add_argument("--num_process", "-n", type=str, required=True, help="number of subprocesses to spawn")

args = parser.parse_args()   
batch_script = args.batch_script

# read in batch_job.txt file
process_n = int(args.num_process)


# parse batch script
with open(batch_script, 'r') as infile:
     lines = [line.strip() for line in infile.readlines()]

for ii, line in enumerate(lines):
    lines[ii] = line + ' -j ' + str(ii%process_n)

# multiprocessing using subprocesses         
def run_subprocess(command):
    """Function to run a subprocess."""
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    return stdout, stderr

def chunkify(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# n_chunks = len(lines) // process_n + (1 if len(lines) % process_n else 0)
chunk_size = max(len(lines) // process_n, 1)

with concurrent.futures.ProcessPoolExecutor(max_workers=process_n) as executor:
    # Split lines into chunks
    for chunk in chunkify(lines, chunk_size):
        futures = [executor.submit(run_subprocess, line.split(' ')) for line in chunk]
    
        # Wait for all futures to complete
        for future in concurrent.futures.as_completed(futures):
            stdout, stderr = future.result()  # This will block until the future is complete
            # Process the results here (for example, printing them)
            print("stdout:", stdout.decode().strip())
            print("stderr:", stderr.decode().strip())



