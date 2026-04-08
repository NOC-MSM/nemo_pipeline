"""
validation.py

Description: Validation functions for NEMO Pipeline package.

Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
"""

# -- Import dependencies -- #
from typing import Literal
from pydantic import BaseModel, Field


# Define Pydantic sub-models for each section of config .toml file:
class SLURMSBATCH(BaseModel):
    """
    NEMO Pipeline SLURM SBATCH configuration model.
    """
    # SLURM SBATCH submission directives:
    job_name : str
    time : str
    ntasks : int
    mem : str
    partition : str
    kwargs : dict[str, str] = Field(default_factory=dict)


class SLURMJobs(BaseModel):
    """
    NEMO Pipeline SLURM Job(s) configuration model.
    """
    # Define SLURM job type ("array" or "single"):
    job_type : Literal["array", "single"]
    # Python virtual environment activation command.
    venv_cmd : str

    # Initial, final and step input patterns for SLURM job submission:
    ip_start : int | None = None
    ip_end : int | None = None
    ip_step : int | None = None
    # Maximum number of concurrent SLURM jobs:
    max_concurrent_jobs : int | None = None
    # Kill the entire job array in the event of any job failure:
    kill_on_fail : bool | None = None


class SLURMConfig(BaseModel):
    """
    NEMO Pipeline SLURM configuration model.
    """
    # Directories of SLURM job scripts and logs:
    job_dir: str
    log_dir: str
    log_prefix : str

    # Add nested sub-models:
    sbatch: SLURMSBATCH
    jobs: SLURMJobs


class InputGridT(BaseModel):
    """
    NEMO Pipeline Input gridT configuration model.
    """
    # NEMO T-grid (scalar) variables:
    filepath : str | list[str] | None = None
    vars : list[str] | list[list[str]] | None = None
    chunks : dict[str, int] = Field(default_factory=dict)


class InputIcemod(BaseModel):
    """
    NEMO Pipeline Input icemod configuration model.
    """
    # NEMO icemod (sea-ice) variables:
    filepath : str | list[str] | None = None
    vars : list[str] | list[list[str]] | None = None
    chunks : dict[str, int] = Field(default_factory=dict)


class InputGridU(BaseModel):
    """
    NEMO Pipeline Input gridU configuration model.
    """
    # NEMO U-grid (zonal vector) variables:
    filepath : str | list[str] | None = None
    vars : list[str] | list[list[str]] | None = None
    chunks : dict[str, int] = Field(default_factory=dict)


class InputGridV(BaseModel):
    """
    NEMO Pipeline Input gridV configuration model.
    """
    # NEMO V-grid (meridional vector) variables:
    filepath : str | list[str] | None = None
    vars : list[str] | list[list[str]] | None = None
    chunks : dict[str, int] = Field(default_factory=dict)


class InputGridW(BaseModel):
    """
    NEMO Pipeline Input gridW configuration model.
    """
    # NEMO W-grid (vertical vector) variables:
    filepath : str | list[str] | None = None
    vars : list[str] | list[list[str]] | None = None
    chunks : dict[str, int] = Field(default_factory=dict)


class InputGridF(BaseModel):
    """
    NEMO Pipeline Input gridF configuration model.
    """
    # NEMO F-grid (vorticity vector) variables:
    filepath : str | list[str] | None = None
    vars : list[str] | list[list[str]] | None = None
    chunks : dict[str, int] = Field(default_factory=dict)


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

    # Add nested sub-models for each NEMO grid type:
    gridT : InputGridT
    gridU : InputGridU
    gridV : InputGridV
    gridW : InputGridW
    gridF : InputGridF
    icemod : InputIcemod


class DiagnosticConfig(BaseModel):
    """
    NEMO Pipeline diagnostic configuration model.
    """
    # Define diagnostic to be computed using NEMODataTree:
    diagnostic: dict[str, str]
    kwargs: dict = Field(default_factory=dict)


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
