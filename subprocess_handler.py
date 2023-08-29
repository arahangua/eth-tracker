import os,sys
import subprocess
import numpy as np
# read in batch_job.txt file
process_n = 50

with open('./batch_job.txt', 'r') as infile:
     lines = [line.strip() for line in infile.readlines()]

for ii, line in enumerate(lines):
    lines[ii] = line + ' -j ' + str(ii%process_n)
   

n_chunk = int(np.ceil(len(lines)/process_n))
curr_point = 0


for curr_ in np.arange(n_chunk)+1:
    if(curr_==n_chunk):
        modulo = len(lines)%process_n
        job_indices = np.arange(curr_point, curr_point+modulo, 1)
    
    else:
        job_indices = np.arange(curr_point, curr_point+process_n, 1)
    


    processes=[]
    for ind in job_indices:
        parsed = lines[ind].split(' ')
        process = subprocess.Popen(parsed)
        processes.append(process)

    # wait for this batch to finish.
    for process in processes:
        process.wait()