"""
Utilities for writing and running Slurm/PBS jobs.

TODO:
- Test with Slurm
"""

import datetime
import os
import uuid

try:
    DEFAULT_JOB_DIR = os.environ['CLUSTER_UTILS_JOB_DIR']
except KeyError:
    DEFAULT_JOB_DIR = '~/scratch/gregdeon/jobs'
    print(f'CLUSTER_UTILS_JOB_DIR not set; using default of {DEFAULT_JOB_DIR}')

def format_walltime_str(walltime_mins):
    """
    Format a walltime in minutes as a string.
    
    Arguments:
    - walltime_mins (int): walltime in minutes
    
    Returns: string formatted as [D-]HH:MM:SS

    Sample usage:
    ```
    >>> format_walltime_str(60)
    '01:00:00'
    >>> format_walltime_str(1440)
    '1-00:00:00'
    ```
    """
    t = datetime.timedelta(minutes=walltime_mins)
    days = t.days
    hours = t.seconds // 3600
    minutes = (t.seconds // 60) % 60

    if t.days == 0:
        return f'{hours:02d}:{minutes:02d}:00'
    else:
        return f'{days}-{hours:02d}:{minutes:02d}:00'

def get_header(job_name, allocation, output_dir, walltime_mins=180, nodes=1, cpus=1, mem_gb=16, gpus=0, gpu_mem_gb=None, array_len=None, slurm=False):
    """
    Get the header for a PBS/Slurm script.
    
    Arguments:
    - job_name (str): name of job
    - allocation (str): allocation name
    - output_dir (str): directory for output files
    - walltime_mins (int): walltime in minutes
    - nodes (int): number of nodes
    - cpus (int): number of CPUs per node
    - mem_gb (int; default=16): memory per node (in GB)
    - gpus (int; default=0): number of gpus per node
    - gpu_mem_gb (int; default=None): memory in GB per gpu
    - array_len (int; default=None): number of array jobs. If None, then not an array job.
    - slurm (bool; default=False): whether to use Slurm instead of PBS

    Returns: header string
    """

    walltime_str = format_walltime_str(walltime_mins)
    if slurm:
        if array_len is not None: # array job
            output_path = f'{output_dir}/%x-%j-%a.txt'
            array_str = f'#SBATCH --array=1-{array_len}\n'
        else:
            output_path = f'{output_dir}/%x-%j.txt'
            array_str = ''

        gpu_mem_str = f'#SBATCH --mem-per-gpu={gpu_mem_gb}gb' if gpu_mem_gb is not None else ''

        return f'''#!/bin/bash
#SBATCH --time={walltime_str}
#SBATCH --nodes={nodes}
#SBATCH --ntasks-per-node={cpus}
#SBATCH --gres=gpu:{gpus}
#SBATCH --mem={mem_gb}gb
{gpu_mem_str}
#SBATCH --job-name={job_name}
#SBATCH --account={allocation}
#SBATCH --output={output_dir}/%x-%j.txt
{array_str}'''

    else:
        if array_len is not None: # array job
            output_path = f'{output_dir}/^array_index^.txt'
            array_str = f'#PBS -J 1-{array_len}\n'
        else: # single job
            output_path = f'{output_dir}/out.txt'
            array_str = ''
            
        gpu_str = f':ngpus={gpus}' if gpus > 0 else ''
        gpu_mem_str = f':gpu_mem={gpu_mem_gb}gb' if gpu_mem_gb is not None else ''

        return f'''#!/bin/bash
#PBS -l walltime={walltime_str},select={nodes}:ncpus={cpus}:mem={mem_gb}gb{gpu_str}{gpu_mem_str}
#PBS -N {job_name}
#PBS -A {allocation}
#PBS -j oe 
#PBS -o {output_path}
{array_str}'''

def get_job_string(job=None, job_array=None, prefix='', slurm=False, **header_kwargs):
    """
    Build a job string from a header and a job command.
    """
    if job is None and job_array is None:
        raise ValueError('Must specify either job or job_array')
    if job is not None and job_array is not None:
        raise ValueError('Cannot specify both job and job_array')


    if job is not None:
        array_len = None
        job_string = job
    else: # job_array is not None
        array_len = len(job_array)
        header_kwargs['array_len'] = array_len
        job_strings = [f'{i+1})\n{job}\n;;\n' for i, job in enumerate(job_array)]
        if slurm:
            job_string = f'case $SLURM_ARRAY_TASK_ID in\n{"".join(job_strings)}\nesac'
        else: # PBS
            job_string = f'case $PBS_ARRAY_INDEX in\n{"".join(job_strings)}\nesac'

    header = get_header(**header_kwargs)
    return f'''{header}
{prefix}
{job_string}
'''

def run_job(fname=None, dry_run=False, verbose=True, **job_kwargs):
    """
    
    Arguments:
    - fname (str; default=None): path to job file. If None, then use {DEFAULT_JOB_DIR}/{job_name}/jobs/{uuid}.pbs.
    - dry_run (bool; default=False): if True, then write the job file without running it
    - job_kwargs: keyword arguments to get_job_string
    """
    # randomly generate a UUID for paths
    job_uuid = uuid.uuid1()

    if fname is None:
        job_name = job_kwargs['job_name']
        fname = os.path.join(DEFAULT_JOB_DIR, job_name, 'jobs', f'{job_uuid}.pbs')
    if 'output_dir' not in job_kwargs:
        job_kwargs['output_dir'] = os.path.join(os.path.dirname(os.path.dirname(fname)), 'output', f'{job_uuid}')

    # create directories if they don't exist
    os.makedirs(os.path.dirname(fname), exist_ok=True)
    os.makedirs(job_kwargs['output_dir'], exist_ok=True)
            
    # write job to file
    job_string = get_job_string(**job_kwargs)
    with open(fname, 'w+', newline='\n') as f:
        f.write(job_string)
    if verbose:
        print(f'wrote job to {fname}')

    if dry_run:
        if verbose:
            print('dry run; not running job')
    else:
        if job_kwargs.get('slurm', False):
            os.system(f'sbatch {fname}')
        else: # PBS
            os.system(f'qsub {fname}')
        if verbose:
            print(f'submitted job')


"""Legacy code below"""
def write_job(fname, job, header):
    """
    Write a single job PBS script.
    
    Arguments: 
    - fname: .pbs file to create
    - header: string with directives and common setup
    - job: strings with command to run
    - platform: unused (for compatibility with write_array_job API)
    """
    with open(fname, 'w+', newline='\n') as f:
        f.write(header)
        f.write(job)

    print(f'wrote job to {fname}')

def write_array_job(fname, header, jobs, platform='slurm'):
    """
    Write an array job PBS script.
    
    Arguments:
    - fname: .pbs file to create
    - header: string with directives and common setup
    - job_list: list of strings with commands to run for each array job
    - platform: 'slurm' or 'pbs'
    
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
        write_array_job('jobs/array_job.pbs', header, job_list, platform='pbs')
        ```
    """
    # Look up task ID
    task_id = {
        'pbs': '$PBS_ARRAY_INDEX',
        'slurm': '$SLURM_ARRAY_TASK_ID'
    }[platform.lower()]

    with open(fname, 'w+', newline='\n') as f:
        f.write(header)
        f.write(f'case {task_id} in\n')
        for job_num, job_str in enumerate(jobs):
            f.write(f'{job_num+1})\n{job_str}\n;;\n')
        f.write('esac\n')

    print(f'wrote {len(jobs)} jobs to {fname}')
