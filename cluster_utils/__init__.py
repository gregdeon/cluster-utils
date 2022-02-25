def write_job(fname, header, job):
    """
    Write a single job PBS script.
    
    Arguments: 
    - fname: .pbs file to create
    - header: string with directives and common setup
    - job: strings with command to run
    """
    with open(fname, 'w+', newline='\n') as f:
        f.write(header)
        f.write(job)

    print(f'wrote job to {fname}')

def write_array_job(fname, header, jobs):
    """
    Write an array job PBS script.
    
    Arguments:
    - fname: .pbs file to create
    - header: string with directives and common setup
    - job_list: list of strings with commands to run for each array job
    
    Sample usage:
    ```
        job_list = [
            f'python script.py --seed {i}' for i in range(10)
        ]
        
        header = f'''#!/bin/bash
        #PBS -l walltime=1:00:00,select=1:ncpus=1:ngpus=1:mem=32gb
        #PBS -N sample-job
        #PBS -A st-kevinlb-1-gpu
        #PBS -m abe
        #PBS -M gregdeon@cs.ubc.ca
        #PBS -o job_outputs/output-^array_index^.txt
        #PBS -e job_output/error-^array_index^.txt
        #PBS -J 1-{len(job_list)}

        module load python3
        '''
        write_array_job('jobs/array_job.pbs', header, job_list)
        ```
    """
    with open(fname, 'w+', newline='\n') as f:
        f.write(header)
        f.write('case $PBS_ARRAY_INDEX in\n')
        for job_num, job_str in enumerate(jobs):
            f.write(f'{job_num+1})\n{job_str}\n;;\n')
        f.write('esac\n')

    print(f'wrote {len(jobs)} jobs to {fname}')
