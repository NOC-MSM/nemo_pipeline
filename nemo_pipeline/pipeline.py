"""
pipeline.py

Description: I/O functions for NEMO Pipeline package.

Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
Date Created: 28/10/2025
"""

# -- Import Dependencies -- #
import glob
import logging
import configparser
import xarray as xr
from nemo_cookbook import NEMODataTree

from nemo_pipeline.utils import get_output_filename, get_config, parse_chunks


# -- Define Utility Functions -- #
def open_domain_ds(
    filepath: str
    ) -> xr.Dataset:
    """
    Open NEMO model domain configuration dataset.

    Parameters:
    -----------
    filepath : str
        Filepath to NEMO model domain configuration netCDF file.

    Returns:
    --------
    xr.Dataset
        NEMO model domain configuration dataset.
    """
    # Validate inputs:
    if not isinstance(filepath, str):
        raise TypeError("domain_cfg filepath must be a string.")

    # Open dataset:
    ds_domain = xr.open_dataset(filepath, engine="netcdf4")
    # Update dimensions to NEMO standard names:
    if "z" in ds_domain.dims:
        ds_domain = ds_domain.rename({"z": "nav_lev"})

    return ds_domain.squeeze()


def open_grid_ds(
    filepath: str,
    variables: list[str] | None = None
    ) -> xr.Dataset:
    """
    Open NEMO model grid dataset from a netCDF file(s).

    Parameters:
    -----------
    filepath : str
        Filepath pattern to NEMO model grid netCDF file(s).
    variables : list of str, optional
        List of variable names to load from the dataset. If None, all variables are loaded.

    Returns:
    --------
    xr.Dataset
        NEMO model grid dataset.
    """
    # Validate inputs:
    if not isinstance(filepath, str):
        raise TypeError("filepath must be a string.")
    if not isinstance(variables, list):
        raise TypeError("variables must be a list of strings.")

    filepaths = glob.glob(filepath)
    if len(filepaths) == 0:
        raise FileNotFoundError(f"No files found matching filepath: {filepath}")

    # Open NEMO model grid dataset with specified variables only:
    if len(filepaths) == 1:
        try:
            if variables is None:
                ds_grid = xr.open_dataset(filepaths[0], engine="netcdf4")
            else:
                ds_grid = xr.open_dataset(filepaths[0], engine="netcdf4")[variables]
        except Exception as e:
            raise RuntimeError(f"Failed to open NEMO model grid dataset: {e}")

    else:
        try:           
            if variables is None:
                ds_grid = xr.open_mfdataset(filepaths,
                                            data_vars="minimal",
                                            compat="no_conflicts",
                                            parallel=False,
                                            engine="netcdf4"
                                            )
            else:
                ds_grid = xr.open_mfdataset(filepaths,
                                            data_vars="minimal",
                                            compat="no_conflicts",
                                            parallel=False,
                                            engine="netcdf4",
                                            preprocess=lambda ds: ds[variables]
                                            )
        except Exception as e:
            raise RuntimeError(f"Failed to open NEMO model grid dataset: {e}")

    return ds_grid


def open_nemo_datasets(
    config: configparser.ConfigParser,
    ) -> dict[str, xr.Dataset]:
    """
    Open NEMO model domain configuration and grid datasets.

    Parameters:
    -----------
    config : configparser.ConfigParser
        ConfigParser object containing NEMO model output file paths.

    Returns:
    --------
    dict[str, xr.Dataset]
        Dictionary of NEMO model domain & grid datasets.
    """
    # Verify input:
    if not isinstance(config, configparser.ConfigParser):
        raise TypeError("config must be a configparser.ConfigParser object.")

    # Open NEMO domain configuration:
    inputs = config["INPUTS"]
    domain_filepath = inputs.get("domain_filepath", None)
    if domain_filepath is None:
        raise ValueError("domain_filepath must be specified in the config file.")
    else:
        d_nemo = {}
        d_nemo["domain"] = open_domain_ds(domain_filepath)
        logging.info("--> Completed: Opened NEMO model domain_cfg dataset")

    # Define NEMO grid filepaths & variables from config:
    grid_filepaths = {
        "gridT": inputs.get("gridT_filepath", None),
        "gridU": inputs.get("gridU_filepath", None),
        "gridV": inputs.get("gridV_filepath", None),
        "gridW": inputs.get("gridW_filepath", None),
        "icemod": inputs.get("icemod_filepath", None),
    }
    grid_variables = {
        "gridT": inputs.get("gridT_vars", None),
        "gridU": inputs.get("gridU_vars", None),
        "gridV": inputs.get("gridV_vars", None),
        "gridW": inputs.get("gridW_vars", None),
        "icemod": inputs.get("icemod_vars", None),
    }

    # Open NEMO grid datasets:
    for grid in grid_filepaths:
        filepath = grid_filepaths[grid]
        var_names = grid_variables[grid]
        if filepath is not None:
            if var_names is not None:
                variables = [var.strip() for var in var_names.split(",")]
            else:
                variables = None
            # Open grid dataset with specified variables:
            d_nemo[grid] = open_grid_ds(filepath, variables)
            logging.info(f"--> Completed: Opened NEMO model {grid} dataset")

    return d_nemo


def create_nemodatatree(
    d_nemo: dict[str, xr.Dataset],
    iperio: bool = False,
    nftype: str | None = None,
    read_mask: bool = False
    ) -> NEMODataTree:
    """
    Create NEMODataTree object from NEMO model domain & grid datasets.

    Parameters:
    -----------
    d_nemo : dict[str, xr.Dataset]
        Dictionary of NEMO model domain & grid datasets.
    
    iperio: bool = False
        Zonal periodicity of the parent domain.

    nftype: str, optional
        Type of north fold lateral boundary condition to apply. Options are 'T' for T-point pivot or 'F' for F-point
        pivot. By default, no north fold lateral boundary condition is applied (None).

    read_mask: bool = False
        If True, read NEMO model land/sea mask from domain files. Default is False, meaning masks are computed from top_level and bottom_level domain variables.

    Returns:
    --------
    NEMODataTree
        NEMODataTree object containing NEMO model data.
    """
    # Validate input:
    if not isinstance(d_nemo, dict) & all(isinstance(ds, xr.Dataset) for ds in d_nemo.values()):
        raise TypeError("d_nemo must be a dictionary of xr.Dataset objects.")

    # Create NEMODataTree object:
    datasets = {"parent": d_nemo}
    nemo = NEMODataTree.from_datasets(datasets=datasets,
                                      iperio=iperio,
                                      nftype=nftype,
                                      read_mask=read_mask
                                      )

    return nemo


def save_nemo_diagnostics(
    ds_out: xr.Dataset,
    output_dir: str,
    output_name: str,
    file_format: str,
    date_format: str,
    chunks: dict | None = None,
    ) -> None:
    """
    Save NEMO Pipeline output dataset to file.

    Parameters:
    -----------
    ds_out : xr.Dataset
        NEMO Pipeline output dataset.
    output_dir : str
        Directory to save output file.
    output_name : str
        Name of output file (without extension).
    file_format : str
        Output file format. Options are 'netcdf' or 'zarr'.
    date_format : str
        Date format for time dimension in output filename.
        Options are 'Y' (YYYY), 'M' (YYYY-MM) or 'D' (YYYY-MM-DD).
    chunks : dict, optional
        Dictionary defining chunk sizes for output dataset.
        Default is None, meaning no chunking is applied.

    Returns:
    --------
    str
        Filepath to saved NEMO Pipeline output file.
    """
    # Validate inputs:
    if not isinstance(ds_out, xr.Dataset):
        raise TypeError("ds_out must be an xr.Dataset.")
    if chunks is not None and not isinstance(chunks, dict):
        raise TypeError("chunks must be a dictionary.")
    if not isinstance(output_dir, str):
        raise TypeError("output_dir must be a string.")
    if not isinstance(output_name, str):
        raise TypeError("output_name must be a string.")
    if file_format not in ["netcdf", "zarr"]:
        raise ValueError("file_format must be either 'netcdf' or 'zarr'.")

    # Define output filepath:
    output_filepath = get_output_filename(
        ds_out=ds_out,
        output_dir=output_dir,
        output_name=output_name,
        file_format=file_format,
        date_format=date_format
        )

    # Apply chunking if specified:
    if chunks is not None:
        ds_out = ds_out.chunk(chunks)

    # Write NEMO diagnostic(s) to output file:
    if file_format == "netcdf":
        ds_out.to_netcdf(path=output_filepath, unlimited_dims="time_counter", mode="w")
    elif file_format == "zarr":
        ds_out.to_zarr(store=output_filepath, mode="w")

    return output_filepath


def describe_nemo_pipeline(
    args: dict
    ) -> str:
    """
    Describe & validate NEMO Pipeline using config.

    Parameters:
    -----------
    args : dict
        Command line arguments.

    Returns:
    --------
    str
        Description of NEMO Pipeline package.
    """
    logging.info("==== Inputs ====")
    # Read config file:
    config = get_config(args=args)
    logging.info(f"Read validated config file --> {args['config_file']}")

    # NEMO model domain dataset:
    logging.info("Read NEMO model domain & grid datasets:")
    inputs = config["INPUTS"]
    domain_filepath = inputs.get("domain_filepath", None)
    logging.info(f"* Open NEMO model domain_cfg dataset --> {domain_filepath}")

    # NEMO model grid filepaths & variables:
    grid_filepaths = {
        "gridT": inputs.get("gridT_filepath", None),
        "gridU": inputs.get("gridU_filepath", None),
        "gridV": inputs.get("gridV_filepath", None),
        "gridW": inputs.get("gridW_filepath", None),
        "icemod": inputs.get("icemod_filepath", None),
    }
    grid_variables = {
        "gridT": inputs.get("gridT_vars", None),
        "gridU": inputs.get("gridU_vars", None),
        "gridV": inputs.get("gridV_vars", None),
        "gridW": inputs.get("gridW_vars", None),
        "icemod": inputs.get("icemod_vars", None),
    }
    for grid in grid_filepaths:
        filepath = grid_filepaths[grid]
        var_names = grid_variables[grid]
        if filepath is not None:
            if var_names is not None:
                variables = [var.strip() for var in var_names.split(",")]
            else:
                variables = None
            logging.info(f"* Open {variables} from NEMO model grid dataset --> {filepath}")

    # NEMODataTree:
    logging.info("Create NEMODataTree from NEMO datasets using:")
    logging.info(f"* iperio = {config.getboolean('INPUTS', 'iperio')}")
    logging.info(f"* nftype = {config.get('INPUTS', 'nftype')}")
    logging.info(f"* read_mask = {config.getboolean('INPUTS', 'read_mask')}")

    logging.info("==== Diagnostics ====")
    diag_name = config.get("DIAGNOSTICS", "diagnostic_name")
    logging.info(f"Calculate NEMO offline diagnostic --> {diag_name}()")

    logging.info("==== Outputs ====")
    logging.info(f"Save NEMO diagnostic(s) to {config.get('OUTPUTS', 'format')} file:")
    # Parse config chunking str into dict:
    chunks = parse_chunks(chunks_str=config.get("OUTPUTS", "chunks"))
    logging.info(f"* Output Directory = {config.get('OUTPUTS', 'output_dir')}")
    logging.info(f"* Output Dataset Chunks = {chunks}")
    # Determine output file name:
    if config.get("OUTPUTS", "format") == "netcdf":
        extension = "nc"
    else:
        extension = "zarr"
    logging.info(f"* Output File Name = {config.get('OUTPUTS', 'output_name')}_YYYY-MM_YYYY-MM.{extension}")
