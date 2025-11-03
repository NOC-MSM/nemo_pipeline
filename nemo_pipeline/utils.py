"""
utils.py

Description: Utility functions for NEMO Pipeline package.

Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
Date Created: 28/10/2025
"""

# -- Import dependencies -- #
import cftime
import tomllib
import importlib
import numpy as np
import xarray as xr
from pathlib import Path
from typing import Literal
from pydantic import BaseModel, Field


# Define Pydantic sub-models for each section of config .toml file:
class SLURMConfig(BaseModel):
    """
    NEMO Pipeline SLURM configuration model.
    """
    # Directories of SLURM job scripts and logs:
    job_dir: str
    log_dir: str
    log_prefix : str
    # SLURM batch job submission parameters:
    sbatch_job_name : str
    sbatch_time : str
    sbatch_partition : str
    sbatch_ntasks : int
    sbatch_mem : str
    # Define the initial, final and step input patterns for batch job submission:
    ip_start : int
    ip_end : int
    ip_step : int
    # Define maximum number of concurrent SLURM jobs.
    max_concurrent_jobs : int
    # Define Python virtual environment activation command.
    venv_cmd : str

class InputConfig(BaseModel):
    """
    NEMO Pipeline Input configuration model.
    """
    # Define NEMO ocean model filepaths used to construct NEMODataTree object:
    nemo_dir : str
    domain_filepath : str
    # Domain Properties:
    iperio : bool
    nftype : Literal["T", "F"]
    read_mask : bool
    # CMORISED variables:
    cmorised : bool
    # NEMO T-grid (scalar) variables:
    gridT_filepath : str | list[str] | None = None
    gridT_vars : list[str] | None = None
    # NEMO U-grid (zonal vector) variables:
    gridU_filepath : str | list[str] | None = None
    gridU_vars : list[str] | None = None
    # NEMO V-grid (meridional vector) variables:
    gridV_filepath : str | list[str] | None = None
    gridV_vars : list[str] | None = None
    # NEMO W-grid (vertical vector) variables:
    gridW_filepath : str | list[str] | None = None
    gridW_vars : list[str] | None = None
    # NEMO icemod (sea-ice) variables:
    icemod_filepath : str | list[str] | None = None
    icemod_vars : list[str] | None = None


class DiagnosticConfig(BaseModel):
    """
    NEMO Pipeline diagnostic configuration model.
    """
    # Define diagnostic to be computed using NEMODataTree:
    diagnostic: dict[str, str]


class OutputConfig(BaseModel):
    """
    NEMO Pipeline output configuration model.
    """
    # Define NEMO ocean model pipeline output file:
    output_dir : str
    output_name : str
    format : Literal["netcdf", "zarr"]
    chunks : dict[str, int] = Field(default_factory=dict)
    date_format : Literal["Y", "M", "D"]


class AppConfig(BaseModel):
    """
    NEMO Pipeline CLI configuration model.
    """
    slurm: SLURMConfig
    inputs: InputConfig
    diagnostics: DiagnosticConfig
    outputs: OutputConfig


def load_config(args: dict) -> AppConfig:
    """
    Load NEMO Pipeline configuration .toml file.

    Uses Pydantic models to parse and validate
    configuration .toml files.

    Parameters:
    -----------
    args : dict
        Command line arguments.
    
    Returns:
    --------
    dict
        Configuration parameters.
    """
    # Open config .toml file:
    path = Path(args['config_file'])
    with open(path, "rb") as f:
        data = tomllib.load(f)

    # Parse and validate config data using Pydantic models:
    config = AppConfig(**data)
    # Convert config params to dict:
    d_config = config.model_dump(mode="json")

    return d_config


def load_diagnostic(
    module_name: str,
    function_name: str
    ):
    """
    Dynamically load user-defined diagnostic function.

    Parameters:
    -----------
    module_name : str
        Module name of user-defined diagnostic function.
    function_name : str
        Function name of user-defined diagnostic function.

    Returns:
    --------
    function
        User-defined diagnostic function.
    """
    # Validate inputs:
    if not isinstance(module_name, str):
        raise TypeError("module_name must be a string.")
    if not isinstance(function_name, str):
        raise TypeError("function_name must be a string.")

    if not module_name.startswith("nemo_pipeline.diagnostics"):
        raise ValueError(f"module {module_name} must be inside the 'nemo_pipeline.diagnostics' namespace.")
    if not module_name.endswith(".core") and not module_name.endswith(".usrdef"):
        raise ValueError("diagnostics must be defined in the 'nemo_pipeline.diagnostics.core' or 'nemo_pipeline.diagnostics.usrdef' modules.")

    # Dynamically import diagnostic function:
    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        raise ImportError(f"Failed to import module '{module_name}': {e}")
    try:
        func = getattr(module, function_name)
    except AttributeError as e:
        raise AttributeError(f"Failed to import function '{function_name}' from module '{module_name}': {e}")

    # Verify that diagnostic is callable:
    if not callable(func):
        raise TypeError(f"'{function_name}' in module '{module_name}' is not callable.")

    return func


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

    # Create date string from CFTime datetime objects:
    if isinstance(time_limits[0], cftime.datetime):
        if date_format == "Y":
            fmt = "%Y"
        elif date_format == "M":
            fmt = "%Y-%m"
        elif date_format == "D":
            fmt = "%Y-%m-%d"
        else:
            raise ValueError(f"Invalid date_format: '{date_format}'. Options are 'Y', 'M', 'D'.")
        date_str = f"{time_limits[0].strftime(fmt)}-{time_limits[1].strftime(fmt)}"

    # Create date string from numpy datetime64:
    elif isinstance(time_limits[0], np.datetime64):
        date_str = f"{np.datetime_as_string(time_limits[0], unit=date_format)}-{np.datetime_as_string(time_limits[1], unit=date_format)}"
    else:
        raise TypeError(f"Invalid type ({type(time_limits[0])}) for dates. Expected cftime.datetime or np.datetime64.")

    # Define output filename:
    if file_format == "netcdf":
        output_filename = f"{output_dir}/{output_name}_{date_str}.nc"
    elif file_format == "zarr":
        output_filename = f"{output_dir}/{output_name}_{date_str}.zarr"

    return output_filename
