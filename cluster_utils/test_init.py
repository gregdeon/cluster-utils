from cluster_utils import format_walltime_str, get_header, get_job_string, run_job

import pytest 

def test_format_walltime_str():
    assert format_walltime_str(0) == "00:00:00"
    assert format_walltime_str(60) == "01:00:00"
    assert format_walltime_str(61) == "01:01:00"
    assert format_walltime_str(1440) == "1-00:00:00"
    assert format_walltime_str(1441) == "1-00:01:00"
    assert format_walltime_str(1440*7) == "7-00:00:00"

def test_get_header():
    header1 = get_header(
        job_name="example_job",
        allocation="allocation_name",
        output_dir="output_dir",
        walltime_mins=60,
        nodes=1,
        cpus=1
    )
    assert header1 == f"""#!/bin/bash
#PBS -l walltime=01:00:00,select=1:ncpus=1:mem=16gb
#PBS -N example_job
#PBS -A allocation_name
#PBS -j oe -o output_dir/$PBS_JOBNAME-$PBS_JOBID.txt
"""

    header2 = get_header(
        job_name="example_job",
        allocation="allocation_name",
        output_dir="output_dir",
        walltime_mins=60,
        nodes=1,
        cpus=1,
        gpus=1,
        gpu_mem_gb=16
    )
    assert header2 == f"""#!/bin/bash
#PBS -l walltime=01:00:00,select=1:ncpus=1:mem=16gb:ngpus=1:gpu_mem=16gb
#PBS -N example_job
#PBS -A allocation_name
#PBS -j oe -o output_dir/$PBS_JOBNAME-$PBS_JOBID.txt
"""

    header3 = get_header(
        job_name="example_job",
        allocation="allocation_name",
        output_dir="output_dir",
        walltime_mins=60,
        nodes=1,
        cpus=1,
        array_len=10
    )
    assert header3 == f"""#!/bin/bash
#PBS -l walltime=01:00:00,select=1:ncpus=1:mem=16gb
#PBS -N example_job
#PBS -A allocation_name
#PBS -j oe -o output_dir/$PBS_JOBNAME-$PBS_JOBID-^array_index^.txt
#PBS -J 1-10
"""

def test_get_header_slurm():
    pass # TODO

def test_get_job_string():
    job_string = get_job_string("echo 'hello world'",
        prefix='module load python',
        job_name="example_job",
        allocation="allocation_name",
        output_dir="output_dir",
        walltime_mins=60,
        nodes=1,
        cpus=1
    )

    assert job_string == f"""#!/bin/bash
#PBS -l walltime=01:00:00,select=1:ncpus=1:mem=16gb
#PBS -N example_job
#PBS -A allocation_name
#PBS -j oe -o output_dir/$PBS_JOBNAME-$PBS_JOBID.txt

module load python
echo 'hello world'
"""

def test_run_job():
    fname = 'example_job.sh'
    run_job(fname, 
        dry_run=True,
        job="echo 'hello world'", 
        prefix='module load python',
        job_name="example_job",
        allocation="allocation_name",
        output_dir="output_dir",
        walltime_mins=60,
        nodes=1,
        cpus=1)
    
    with open(fname, 'r') as f:
        job_string = f.read()

    assert job_string == f"""#!/bin/bash
#PBS -l walltime=01:00:00,select=1:ncpus=1:mem=16gb
#PBS -N example_job
#PBS -A allocation_name
#PBS -j oe -o output_dir/$PBS_JOBNAME-$PBS_JOBID.txt

module load python
echo 'hello world'
"""
