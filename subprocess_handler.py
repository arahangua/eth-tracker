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


n_chunk = int(np.ceil(len(lines)/process_n))
curr_point = 0


# multiprocessing using subprocesses         
def run_subprocess(command):
    """Function to run a subprocess."""
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    return stdout, stderr


curr_point = 0
with concurrent.futures.ProcessPoolExecutor() as executor:
    for curr_ in np.arange(n_chunk) + 1:
        if(len(lines)<=process_n):
            job_indices = np.arange(curr_point, curr_point + len(lines))
            curr_point += len(lines)
        
        elif curr_ == n_chunk:
            modulo = len(lines) % process_n
            job_indices = np.arange(curr_point, curr_point + modulo)
            curr_point += modulo
        
        else:
            job_indices = np.arange(curr_point, curr_point + process_n)
            curr_point += process_n

        # Create a list to hold futures
        futures = []
        for ind in job_indices:
            command = lines[ind].split(' ')
            # Submit subprocesses to the executor
            future = executor.submit(run_subprocess, command)
            futures.append(future)


        # Wait for futures to complete if needed and handle results
        concurrent.futures.wait(futures)
        
        # for future in futures:
        #     stdout, stderr = future.result()
            # Process stdout and stderr if needed