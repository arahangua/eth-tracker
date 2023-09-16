import os,sys
import subprocess
import numpy as np
import argparse 


parser = argparse.ArgumentParser(description="batch job parser for eth-tracker")
parser.add_argument("--batch_script", "-b",  type=str, required=True, help="name of the batch script. Each line in the script should be one of eth-tracker jobs")
   
args = parser.parse_args()   
batch_script = args.batch_script

# read in batch_job.txt file
process_n = 5

with open(batch_script, 'r') as infile:
     lines = [line.strip() for line in infile.readlines()]

for ii, line in enumerate(lines):
    lines[ii] = line + ' -j ' + str(ii%process_n)
   

n_chunk = int(np.ceil(len(lines)/process_n))
curr_point = 0


for curr_ in np.arange(n_chunk)+1:
    if(curr_==n_chunk):
        modulo = len(lines)%process_n
        job_indices = np.arange(curr_point, curr_point+modulo, 1)
        # increment
        curr_point = curr_point+modulo

    else:
        job_indices = np.arange(curr_point, curr_point+process_n, 1)
        # increment
        curr_point = curr_point+process_n


    processes=[]
    for ind in job_indices:
        parsed = lines[ind].split(' ')
        process = subprocess.Popen(parsed)
        processes.append(process)

    # wait for this batch to finish.
    for process in processes:
        process.wait()