"""
submit.py

Description: Batch job submission functions for the
NEMO Pipeline package.

Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
Date Created: 29/10/2025
"""

# -- Import Dependencies -- #
import os
import logging
from pathlib import Path
import subprocess

from nemo_pipeline.utils import get_config

# -- SLURM Job Submission -- #
def submit_slurm_job(
    args: dict,
    ) -> None:
    """
    Submit NEMO Pipeline tasks as a job to a SLURM scheduler.

    Parameters
    ----------
    args : dict
        Command line arguments.
    config : configparser.ConfigParser
        Configuration object containing NEMO Pipeline settings.
    """
    # Verify inputs:
    if not isinstance(args, dict):
        raise TypeError("args must be a dictionary.")
    logging.info("In Progress: Preparing NEMO Pipeline as SLURM job array...")

    # Load NEMO pipeline configuration:
    if ('/' not in args['config_file']):
        args['config_file'] = Path.cwd() / args['config_file']
    config = get_config(args)
    slurm_params = config['SLURM']
    output_params = config['OUTPUTS']

    # Create job, logging & output directories:
    if 'log_dir' not in slurm_params:
        slurm_params['log_dir'] =  Path.cwd() / 'logs'
    if 'job_dir' not in slurm_params:
        slurm_params['job_dir'] =  Path.cwd() / 'jobs'

    os.makedirs(slurm_params['log_dir'], exist_ok=True)
    os.makedirs(slurm_params['job_dir'], exist_ok=True)
    os.makedirs(output_params['output_dir'], exist_ok=True)

    # Get input patterns from filepaths:
    ip_start = slurm_params.getint('ip_start')
    ip_end = slurm_params.getint('ip_end')
    ip_step = slurm_params.getint('ip_step')
    max_concurrent = slurm_params.getint('max_concurrent_jobs')
    venv_cmd = slurm_params.get('venv_cmd', None)
    if venv_cmd is None:
        venv_cmd = ""

    # Define nemo_pipeline command with arguments:
    np_cmd = f"nemo_pipeline run --config {args['config_file']} --input_pattern $task_ip --log {slurm_params['log_dir']}/{slurm_params['log_prefix']}_$task_ip.log"

    # Create SLURM job script:
    job_script = f"""#!/bin/bash
#SBATCH --job-name={slurm_params['sbatch.job_name']}
#SBATCH --time={slurm_params['sbatch.time']}
#SBATCH --partition={slurm_params['sbatch.partition']}
#SBATCH --ntasks={slurm_params['sbatch.ntasks']}
#SBATCH --mem={slurm_params['sbatch.mem']}
#SBATCH --output={slurm_params['log_dir']}/%A_%a.out
#SBATCH --array={ip_start}-{ip_end}:{ip_step}%{max_concurrent}

# -- SLURM Array Task ID -- #
task_ip=${{SLURM_ARRAY_TASK_ID}}
echo ---- Running NEMO Pipeline SLURM Job Task $task_ip ----

# -- Activate Python Virtual Environment -- #
{venv_cmd}

# -- Run NEMO Pipeline -- #
{np_cmd}

echo ---- Completed: NEMO Pipeline SLURM Job Task $task_ip ----
    """
    
    # Write job script to file:
    job_script_path = os.path.join(slurm_params['job_dir'], f"{slurm_params['log_prefix']}_nemo_pipeline.slurm")
    with open(job_script_path, 'w') as f:
        f.write(job_script)

    # Submit job script to SLURM scheduler:
    if args['no_submit']:
        logging.info(f"Completed: Created SLURM job script (not submitted) --> {job_script_path}.")
    else:
        result = subprocess.run(["sbatch", job_script_path], capture_output=True, text=True)
        print(result.stdout.strip())
        logging.info(f"Completed: Created & submitted SLURM job script --> {job_script_path}.")
