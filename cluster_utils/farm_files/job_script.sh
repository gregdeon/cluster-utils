#!/bin/bash
#SBATCH --time=12:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4gb
#SBATCH --job-name=bgt_2024-11-08-linear7
#SBATCH --account=st-kevinlb-1

# Don't change this line:
task.run
