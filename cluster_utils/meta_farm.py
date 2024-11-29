"""Utilities for launching job farms using META-Farm.
"""
import os
import shutil
from typing import List, Dict, Optional, Any

from cluster_utils.utils import get_job_string

FILE_DIR = os.path.dirname(os.path.abspath(__file__))
FARM_FILE_DIR = f'{FILE_DIR}/farm_files'

def make_farm(
    farm_dir: str,
    case_list: List[str],
    prefix: str = '',
    job_args: Dict[str, Any] = {},
    final_script: Optional[str] = None,
    final_args: Optional[Dict[str, Any]] = None
) -> None:
    """
    Make a farm.

    Arguments:
    - farm_dir: path to the farm
    - case_list: list of individual cases to run
    - prefix: common code to run at the start of each worker job
    - job_args: dictionary of Slurm directives; see utils.get_header for full list of accepted keys
    - final_script: command to run when the farm is complete (optional)
    - final_args: dictionary of Slurm directives for final script (optional)
    """

    # make farm directory, failing if it already exists
    os.mkdir(farm_dir)

    # copy config.h and single_case.sh
    shutil.copy(f'{FARM_FILE_DIR}/config.h', f'{farm_dir}/config.h')
    shutil.copy(f'{FARM_FILE_DIR}/single_case.sh', f'{farm_dir}/single_case.sh')

    # generate job_script.sh and resubmit_script.sh
    with open(f'{farm_dir}/job_script.sh', 'w') as f:
        f.write(get_job_string('task.run', prefix=prefix, **job_args))
        
    with open(f'{farm_dir}/resubmit_script.sh', 'w') as f:
        f.write(get_job_string('autojob.run', prefix=prefix, **job_args))

    # generate table.dat from case list
    table_string = '\n'.join([f'{i+1} {case_string}' for i, case_string in enumerate(case_list)])
    with open(f'{farm_dir}/table.dat', 'w', newline='\n') as f:
        f.write(table_string)

    # generate final.sh 
    if final_script is not None:
        with open(f'{farm_dir}/final.sh', 'w') as f:
            f.write(get_job_string(final_script, **final_args))

    print(f'Successfully created farm in {farm_dir}')
