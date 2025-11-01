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
import subprocess
from pathlib import Path
from nemo_pipeline.utils import load_config

# -- SLURM Job Submission -- #
def submit_slurm_pipeline(
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
    config = load_config(args=args)
    slurm_params = config['slurm']
    output_params = config['outputs']

    # Create job, logging & output directories:
    if 'log_dir' not in slurm_params:
        slurm_params['log_dir'] =  Path.cwd() / 'logs'
    if 'job_dir' not in slurm_params:
        slurm_params['job_dir'] =  Path.cwd() / 'jobs'

    os.makedirs(slurm_params['log_dir'], exist_ok=True)
    os.makedirs(slurm_params['job_dir'], exist_ok=True)
    os.makedirs(output_params['output_dir'], exist_ok=True)

    # Get input patterns from filepaths:
    ip_start = slurm_params['ip_start']
    ip_end = slurm_params['ip_end']
    ip_step = slurm_params['ip_step']
    max_concurrent = slurm_params['max_concurrent_jobs']
    venv_cmd = slurm_params['venv_cmd']

    # Define nemo_pipeline command with arguments:
    np_cmd = f"nemo_pipeline run --config {args['config_file']} --input_pattern $task_ip --log {slurm_params['log_dir']}/{slurm_params['log_prefix']}_$task_ip.log"

    # Create SLURM job script:
    job_script = f"""#!/bin/bash
#SBATCH --job-name={slurm_params['sbatch_job_name']}
#SBATCH --time={slurm_params['sbatch_time']}
#SBATCH --partition={slurm_params['sbatch_partition']}
#SBATCH --ntasks={slurm_params['sbatch_ntasks']}
#SBATCH --mem={slurm_params['sbatch_mem']}
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
