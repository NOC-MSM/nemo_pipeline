"""
utils.py

Description: Utility functions for NEMO Pipeline package.

Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
Date Created: 28/10/2025
"""

# -- Import dependencies -- #
import numpy as np
import configparser
import xarray as xr


def validate_config(
    config: configparser.ConfigParser
    ) -> None:
    """
    Validate NEMO Pipeline configuration file.

    Parameters:
    -----------
    config : configparser.ConfigParser
        Configuration parser object.
    """
    # 1. Required sections:
    required_sections = ['INPUTS', 'DIAGNOSTICS', 'OUTPUTS']
    for section in required_sections:
        if section not in config.sections():
            raise ValueError(f"missing required section in config .ini file: {section}")

    # 2. Required options in INPUTS section:
    required_input_options = ['domain_filepath', 'iperio', 'nftype', 'read_mask']
    for option in required_input_options:
        if option not in config['INPUTS']:
            raise ValueError(f"missing required option in INPUTS section of config .ini file: {option}")

    # 3. Required options in OUTPUTS section:
    required_output_options = ['output_dir', 'output_name', 'format', 'date_format', 'chunks']
    for option in required_output_options:
        if option not in config['OUTPUTS']:
            raise ValueError(f"missing required option in OUTPUTS section of config .ini file: {option}")

    # 4. Optional SLURM section:
    if 'SLURM' in config.sections():
        slurm_options = ['job_dir', 'log_dir', 'ip_start', 'ip_end', 'ip_step', 'max_concurrent_jobs', 'sbatch.job_name', 'sbatch.time', 'sbatch.partition', 'sbatch.ntasks', 'sbatch.mem']
        for option in slurm_options:
            if option not in config['SLURM']:
                raise ValueError(f"missing required option in SLURM section of config .ini file: {option}")


def get_config(args):
    """
    Read NEMO Pipeline configuration file.

    Parameters:
    -----------
    args : dict
        Command line arguments.
    
    Returns:
    --------
    configparser.ConfigParser
        Configuration parser object.
    """
    # Read config file - allow full-line comments only:
    config = configparser.ConfigParser(inline_comment_prefixes=())
    config.read(args['config_file'])

    # Validate config file:
    if not config.sections():
        raise ValueError("Config file is empty or invalid.")
    validate_config(config)

    return config


def parse_chunks(
    chunks_str: str
    ) -> dict:
    """
    Parse output chunk string from config file into dictionary.

    Parameters:
    -----------
    chunks_str : str
        Chunking string from config file, e.g. "time_counter:10, deptht:20".

    Returns:
    --------
    dict
        Dictionary defining chunk sizes for each output dimension.
    """
    # Validate input:
    if chunks_str == 'None':
        chunks = None
    else:
        if not isinstance(chunks_str, str):
            raise TypeError("chunks_str must be a string.")

        # Parse chunking string into dictionary:
        chunks = {chunk.split(':')[0].strip(): int(chunk.split(':')[1]) for chunk in chunks_str.split(',')}

    return chunks


def get_output_filename(
    ds_out: xr.Dataset,
    output_dir: str,
    output_name: str,
    file_format: str,
    date_format: str
    ) -> str:
    """
    Define NEMO Pipeline output filename.

    Parameters:
    -----------
    ds_out : xr.Dataset
        Output xarray Dataset.
    output_dir : str
        Directory to save output file.
    output_name : str
        Prefix of output file name.
    file_format : str
        Output file format. Options are 'netcdf' or 'zarr'.
    date_format : str
        Date format for datetime limits in output filename.
        Options are 'Y' (YYYY), 'M' (YYYY-MM) or 'D' (YYYY-MM-DD).
    """
    # Validate inputs:
    if not isinstance(ds_out, xr.Dataset):
        raise TypeError("ds_out must be an xr.Dataset.")
    if not isinstance(output_dir, str):
        raise TypeError("output_dir must be a string.")
    if not isinstance(output_name, str):
        raise TypeError("output_name must be a string.")
    if file_format not in ["netcdf", "zarr"]:
        raise ValueError("file_format must be either 'netcdf' or 'zarr'.")

    # Define time-limits of output dataset:
    time_limits = ds_out['time_counter'].values[[0, -1]]
    time_start = np.datetime_as_string(time_limits[0], unit=date_format)
    time_end = np.datetime_as_string(time_limits[1], unit=date_format)

    # Define output filename:
    if file_format == "netcdf":
        output_filename = f"{output_dir}/{output_name}_{time_start}_{time_end}.nc"
    elif file_format == "zarr":
        output_filename = f"{output_dir}/{output_name}_{time_start}_{time_end}.zarr"

    return output_filename
    