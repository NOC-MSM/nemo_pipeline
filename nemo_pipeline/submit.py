"""
submit.py

Description: Batch job submission functions for the
NEMO Pipeline package.

Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
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
    Submit NEMO Pipeline as a single or array job to SLURM scheduler.

    Parameters
    ----------
    args : dict
        Command line arguments.

    Returns
    -------
    None
    """
    # -- Verify Inputs -- #
    if not isinstance(args, dict):
        raise TypeError("args must be a dictionary.")

    # Load NEMO pipeline configuration:
    if ('/' not in args['config_file']):
        args['config_file'] = Path.cwd() / args['config_file']
    config = load_config(args=args)
    slurm_params = config['slurm']
    output_params = config['outputs']

    job_type = slurm_params['jobs']['job_type']
    if job_type == 'single':
        logging.info("In Progress: Preparing NEMO Pipeline as a single SLURM job...")
    elif job_type == 'array':
        logging.info("In Progress: Preparing NEMO Pipeline as SLURM array job...")

    # Create job, logging & output directories:
    if 'log_dir' not in slurm_params:
        slurm_params['log_dir'] =  Path.cwd() / 'logs'
    if 'job_dir' not in slurm_params:
        slurm_params['job_dir'] =  Path.cwd() / 'jobs'

    os.makedirs(slurm_params['log_dir'], exist_ok=True)
    os.makedirs(slurm_params['job_dir'], exist_ok=True)
    os.makedirs(output_params['output_dir'], exist_ok=True)

    # Collect SLURM job parameters:
    venv_cmd = slurm_params['jobs']['venv_cmd']
    if job_type == 'array':
        ip_start = slurm_params['jobs']['ip_start']
        ip_end = slurm_params['jobs']['ip_end']
        ip_step = slurm_params['jobs']['ip_step']
        max_concurrent = slurm_params['jobs']['max_concurrent_jobs']

    # -- Define nemo_pipeline command with arguments -- #
    if job_type == 'single':
        if args['input_pattern'] == "":
            # Single SLURM job without input pattern argument:
            np_cmd = f"nemo_pipeline run {args['config_file']} --log {slurm_params['log_dir']}/{slurm_params['log_prefix']}.log"
        else:
            # Single SLURM job with input pattern argument:
            np_cmd = f"nemo_pipeline run {args['config_file']} --input-pattern {args['input_pattern']} --log {slurm_params['log_dir']}/{slurm_params['log_prefix']}.log"
    elif job_type == 'array':
        # SLURM array job with input pattern argument defined by SLURM_ARRAY_TASK_ID:
        np_cmd = f"nemo_pipeline run {args['config_file']} --input-pattern $task_ip --log {slurm_params['log_dir']}/{slurm_params['log_prefix']}_$task_ip.log"

        # Include check & kill array in the event of job failure:
        if slurm_params['jobs']['kill_on_fail']:
            np_cmd += """\n
# -- Kill Array on Fail -- #
status=$?

if [ $status -ne 0 ]; then
    echo "Task $SLURM_ARRAY_TASK_ID Failed — Cancelling Job Array..."
    scancel $SLURM_ARRAY_JOB_ID
    exit $status
fi
            """

    # -- Create SLURM job directive header -- #
    job_script = f"""#!/bin/bash
#SBATCH --job-name={slurm_params['sbatch']['job_name']}
#SBATCH --time={slurm_params['sbatch']['time']}
#SBATCH --partition={slurm_params['sbatch']['partition']}
#SBATCH --ntasks={slurm_params['sbatch']['ntasks']}
#SBATCH --mem={slurm_params['sbatch']['mem']}
#SBATCH --output={slurm_params['log_dir']}/%A_%a.out
    """
    # Add optional user-specified SLURM job directives:
    if slurm_params['sbatch']['kwargs'] is not None:
        for directive, value in slurm_params['sbatch']['kwargs'].items():
            job_script += f"\n#SBATCH --{directive}={value}"

    # Add required SLURM job array directives:
    if job_type == 'array':
        job_script += f"\n#SBATCH --array={ip_start}-{ip_end}:{ip_step}%{max_concurrent}"

    # -- Define SLURM job script -- #
    # Add SLURM array task ID variable for job arrays:
    if job_type == 'array':
        job_script += """
\n# -- SLURM Array Task ID -- #
task_ip=${{SLURM_ARRAY_TASK_ID}}
echo ---- Running NEMO Pipeline SLURM Job Task $task_ip ----
        """

    # Add Python venv activation & nemo_pipeline command to job script:
    job_script += f"""
\n# -- Activate Python Virtual Environment -- #
{venv_cmd}

# -- Run NEMO Pipeline -- #
{np_cmd}

echo ---- Completed: NEMO Pipeline SLURM Job Task $task_ip ----
        """
    
    # -- Write job script to file -- #
    job_script_path = os.path.join(slurm_params['job_dir'], f"{slurm_params['log_prefix']}_nemo_pipeline.slurm")
    with open(job_script_path, 'w') as f:
        f.write(job_script)

    # -- Submit job script to SLURM scheduler -- #
    if args['submit']:
        result = subprocess.run(["sbatch", job_script_path], capture_output=True, text=True)
        print(result.stdout.strip())
        logging.info(f"Completed: Created & submitted SLURM job script --> {job_script_path}.")
    else:
        logging.info(f"Completed: Created SLURM job script without submitting --> {job_script_path}.")
