"""
pipeline.py

Description: I/O functions for NEMO Pipeline package.

Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
Date Created: 28/10/2025
"""

# -- Import Dependencies -- #
import glob
import logging
import xarray as xr
from nemo_cookbook import NEMODataTree
from nemo_pipeline.utils import get_output_filename, load_config, load_diagnostic


# -- Define Utility Functions -- #
def create_grid_filepaths(
    config : dict,
    args: dict
    ) -> dict[str, str]:
    """
    Create dictionary of NEMO model grid filepaths from config.

    Parameters:
    -----------
    config : dict
        Configuration parameters, including NEMO model output file paths.
    args : dict
        Command line arguments.

    Returns:
    --------
    dict[str, str]
        Dictionary of NEMO model grid filepaths.
    """
    # Verify input:
    if not isinstance(config, dict):
        raise TypeError("config must be a dictionary.")
    if not isinstance(args, dict):
        raise TypeError("args must be a dictionary.")

    # Define NEMO model grid filepaths from config:
    inputs = config["inputs"]

    if args['input_pattern'] is not None:
        # Replace input pattern in config filepaths:
        grid_filepaths = {}
        for grid in ['gridT', 'gridU', 'gridV', 'gridW', 'icemod']:
            filepath = inputs.get(f"{grid}_filepath", None)
            if filepath is not None:
                if inputs['cmorised']:
                    # List of CMORISED variable filepaths per NEMO model grid:
                    if not isinstance(filepath, list):
                        raise RuntimeError(f"Expected list of filepaths for CMORISED variables, received {type(filepath)}")
                    for n, fpath in enumerate(filepath):
                        if '${nemo_dir}' in fpath:
                            fpath = fpath.replace('${nemo_dir}', inputs['nemo_dir'])
                        if '{ip}' in fpath:
                            fpath = fpath.replace('{ip}', args['input_pattern'])
                            logging.info(f"* Overriding {grid}_filepath[{n}] using input pattern --> {fpath}")
                        filepath[n] = fpath
                else:
                    # Single filepath per NEMO model grid:
                    if not isinstance(filepath, str):
                        raise RuntimeError(f"Expected string filepath for NEMO model grid, received {type(filepath)}")
                    if '${nemo_dir}' in filepath:
                        filepath = filepath.replace('${nemo_dir}', inputs['nemo_dir'])
                    if '{ip}' in filepath:
                        filepath = filepath.replace('{ip}', args['input_pattern'])
                        logging.info(f"* Overriding {grid}_filepath using input pattern --> {filepath}")

            # Add grid filepath:
            grid_filepaths[grid] = filepath
    else:
        # Use filepaths from config file:
        for grid in ['gridT', 'gridU', 'gridV', 'gridW', 'icemod']:
            filepath = inputs.get(f"{grid}_filepath", None)
            if filepath is not None:
                if '{ip}' in filepath:
                    raise RuntimeError(f"Input pattern '{{ip}}' found in {grid}_filepath without specifying --input_pattern flag with nemo_pipeline")
            # Add grid filepath:
            grid_filepaths[grid] = filepath

    return grid_filepaths


def create_variable_lists(
    config: dict
    ) -> dict[str, list[str] | None]:
    """
    Create dictionary of NEMO model grid variable lists from config.

    Parameters:
    -----------
    config : dict
        Configuration parameters, including NEMO model output variables.

    Returns:
    --------
    dict[str, list[str] | None]
        Dictionary of NEMO model grid variable lists.
    """
    # Verify input:
    if not isinstance(config, dict):
        raise TypeError("config must be a dictionary.")

    # Define NEMO model grid variable lists from config:
    inputs = config["inputs"]
    grid_variables = {}
    for grid in ['gridT', 'gridU', 'gridV', 'gridW', 'icemod']:
        grid_variables[grid] = inputs.get(f"{grid}_vars", None)

    return grid_variables

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

    # Define CFDatetimeCoder to decode time coords:
    coder = xr.coders.CFDatetimeCoder(time_unit="s")

    # Open NEMO model grid dataset with specified variables only:
    if len(filepaths) == 1:
        try:
            if variables is None:
                ds_grid = xr.open_dataset(filepaths[0], decode_times=coder, engine="netcdf4")
            else:
                ds_grid = xr.open_dataset(filepaths[0], decode_times=coder, engine="netcdf4")[variables]
        except Exception as e:
            raise RuntimeError(f"Failed to open NEMO model grid dataset: {e}")

    else:
        try:           
            if variables is None:
                ds_grid = xr.open_mfdataset(filepaths,
                                            data_vars="minimal",
                                            compat="no_conflicts",
                                            decode_times=coder,
                                            parallel=False,
                                            engine="netcdf4"
                                            )
            else:
                ds_grid = xr.open_mfdataset(filepaths,
                                            data_vars="minimal",
                                            compat="no_conflicts",
                                            decode_times=coder,
                                            parallel=False,
                                            engine="netcdf4",
                                            preprocess=lambda ds: ds[variables]
                                            )
        except Exception as e:
            raise RuntimeError(f"Failed to open NEMO model grid dataset: {e}")

    return ds_grid


def open_nemo_datasets(
    config: dict,
    args: dict
    ) -> dict[str, xr.Dataset]:
    """
    Open NEMO model domain configuration and grid datasets.

    Parameters:
    -----------
    config : dict
        Configuration parameters, including NEMO model output file paths.
    args : dict
        Command line arguments.

    Returns:
    --------
    dict[str, xr.Dataset]
        Dictionary of NEMO model domain & grid datasets.
    """
    # Verify input:
    if not isinstance(config, dict):
        raise TypeError("config must be a dictionary.")
    if not isinstance(args, dict):
        raise TypeError("args must be a dictionary.")

    # Open NEMO domain configuration:
    inputs = config["inputs"]
    domain_filepath = inputs.get("domain_filepath", None)
    if domain_filepath is None:
        raise ValueError("domain_filepath must be specified in the config file.")
    else:
        d_nemo = {}
        d_nemo["domain"] = open_domain_ds(domain_filepath)
        logging.info("--> Completed: Opened NEMO model domain_cfg dataset")

    # Define NEMO grid filepaths & variable names from config:
    grid_filepaths = create_grid_filepaths(config=config, args=args)
    grid_variables = create_variable_lists(config=config)

    # Open NEMO model grid datasets:
    for grid in grid_filepaths:
        filepath = grid_filepaths[grid]
        var_names = grid_variables[grid]
        if filepath is not None:
            # Open grid dataset with specified variables:
            d_nemo[grid] = open_grid_ds(filepath, var_names)
            logging.info(f"--> Completed: Opened NEMO model {grid} dataset")

    return d_nemo


def open_cmorised_datasets(
    config: dict,
    args: dict
    ) -> dict[str, xr.Dataset]:
    """
    Open NEMO model domain configuration and create
    NEMO grid dataset from CMORISED data variables.

    Parameters:
    -----------
    config : dict
        Configuration parameters, including NEMO model output file paths.
    args : dict
        Command line arguments.

    Returns:
    --------
    dict[str, xr.Dataset]
        Dictionary of NEMO model domain & grid datasets.
    """
    # Verify input:
    if not isinstance(config, dict):
        raise TypeError("config must be a dictionary.")
    if not isinstance(args, dict):
        raise TypeError("args must be a dictionary.")

    # Open NEMO domain configuration:
    inputs = config["inputs"]
    domain_filepath = inputs.get("domain_filepath", None)
    if domain_filepath is None:
        raise ValueError("domain_filepath must be specified in the config file.")
    else:
        d_nemo = {}
        d_nemo["domain"] = open_domain_ds(filepath=domain_filepath)
        logging.info("--> Completed: Opened NEMO model domain_cfg dataset")

    # Define CMORISED variable filepaths & names from config:
    grid_filepaths = create_grid_filepaths(config=config, args=args)
    grid_variables = create_variable_lists(config=config)

    # Create NEMO model grid datasets:
    logging.info("In Progress: Creating NEMO model grid datasets from CMORISED variables.")
    for grid in grid_filepaths:
        filepaths = grid_filepaths[grid]
        var_names = grid_variables[grid]
        if filepaths is not None:
            # Open & merge CMORISED variables into single NEMO model grid dataset:
            if len(filepaths) != len(var_names):
                raise ValueError(f"Number of filepaths ({len(filepaths)}) does not equal number of variables ({len(var_names)}) for grid {grid}.")
            for n, fpath in enumerate(filepaths):
                if n == 0:
                    d_nemo[grid] = open_grid_ds(filepath=fpath, variables=var_names[n])
                else:
                    try:
                        d_nemo[grid][var_names[n]] = open_grid_ds(filepath=fpath, variables=var_names[n])
                    except Exception as e:
                        raise RuntimeError(f"Failed to merge variable {var_names[n]} into NEMO model {grid} dataset: {e}")

            # Update CMORISED dimensions to NEMO standard names:
            try:
                d_nemo[grid] = d_nemo[grid].rename({"i": "x",
                                                    "j": "y",
                                                    "lev": "z",
                                                    "time": "time_counter"
                                                    })
            except Exception as e:
                raise RuntimeError(f"Failed to rename dimensions in NEMO model {grid} dataset: {e}")

            logging.info(f"--> Completed: Created NEMO model {grid} dataset")

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


def run_nemo_pipeline(
    args: dict
    ) -> None:
    """
    Run NEMO Pipeline using specified config .ini file.

    Pipeline Steps:
    1. Read & validate config .ini file.
    2. Open NEMO model domain & grid datasets.
    3. Create NEMODataTree from NEMO datasets.
    4. Calculate NEMO offline diagnostic(s).
    5. Write NEMO diagnostic(s) to output file.

    Parameters:
    -----------
    args : dict
        Command line arguments.
    """
    # === Inputs === #
    logging.info("==== Inputs ====")
    # Load config .toml file:
    config = load_config(args=args)
    logging.info(f"Completed: Read & validated config file -> {args['config_file']}")

    # Open NEMO model domain & grid datasets:
    if config['inputs']['cmorised']:
        logging.info("In Progress: Reading CMORISED NEMO model domain & grid datasets...")
        d_nemo = open_cmorised_datasets(config=config, args=args)
    else:
        logging.info("In Progress: Reading NEMO model domain & grid datasets...")
        d_nemo = open_nemo_datasets(config=config, args=args)
    logging.info("Completed: Reading NEMO model domain & grid datasets")

    # Create NEMODataTree object:
    logging.info("In Progress: Constructing NEMODataTree from NEMO datasets...")
    nemo = create_nemodatatree(d_nemo=d_nemo,
                               iperio=config['inputs']['iperio'],
                               nftype=config['inputs']['nftype'],
                               read_mask=config['inputs']['read_mask']
                               )
    logging.info("Completed: Constructed NEMODataTree from NEMO datasets")

    # === Diagnostics === #
    logging.info("==== Diagnostics ====")
    # Calculate specified NEMO offline diagnostic(s):
    d_diag = config['diagnostics']['diagnostic']
    diag_func = load_diagnostic(
        module_name=d_diag['module'],
        function_name=d_diag['function']
    )
    logging.info(f"In Progress: Calculating NEMO offline diagnostic -> {d_diag['function']}()...")
    ds_diag = diag_func(nemo=nemo)
    logging.info(f"Completed: Calculated NEMO offline diagnostic -> {d_diag['function']}()")

    # === Outputs === #
    logging.info("==== Outputs ====")
    logging.info(f"In Progress: Saving NEMO diagnostic(s) to {config['outputs']['format']} file...")

    # Write NEMO Pipeline output dataset to file:
    output_filepath = save_nemo_diagnostics(
        ds_out=ds_diag,
        output_dir=config['outputs']['output_dir'],
        output_name=config['outputs']['output_name'],
        file_format=config['outputs']['format'],
        date_format=config['outputs']['date_format'],
        chunks=config['outputs']['chunks']
        )

    logging.info(f"Completed: Saved NEMO diagnostic(s) to file -> {output_filepath}")


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
    config = load_config(args=args)
    logging.info(f"Read & validated config file --> {args['config_file']}")

    # NEMO model domain dataset:
    logging.info("Read NEMO model domain & grid datasets:")
    inputs = config["inputs"]
    domain_filepath = inputs.get("domain_filepath", None)
    logging.info(f"* Open NEMO model domain_cfg dataset --> {domain_filepath}")

    # NEMO model grid filepaths & variables:
    grid_filepaths = create_grid_filepaths(config=config, args=args)
    grid_variables = create_variable_lists(config=config)
    for grid in grid_filepaths:
        filepath = grid_filepaths[grid]
        var_names = grid_variables[grid]
        if filepath is not None:
            if config['inputs']['cmorised']:
                logging.info(f"* Create NEMO model grid dataset from CMORISED variables {var_names} --> {filepath}")
            else:
                logging.info(f"* Open {var_names} from NEMO model grid dataset --> {filepath}")

    # NEMODataTree:
    logging.info("Create NEMODataTree from NEMO datasets using:")
    logging.info(f"* iperio = {config['inputs']['iperio']}")
    logging.info(f"* nftype = {config['inputs']['nftype']}")
    logging.info(f"* read_mask = {config['inputs']['read_mask']}")

    logging.info("==== Diagnostics ====")
    d_diag = config['diagnostics']['diagnostic']
    logging.info(f"Calculate NEMO offline diagnostic --> {d_diag['function']}()")

    logging.info("==== Outputs ====")
    logging.info(f"Save NEMO diagnostic(s) to {config['outputs']['format']} file:")
    # Parse config chunking str into dict:
    logging.info(f"* Output Directory = {config['outputs']['output_dir']}")
    logging.info(f"* Output Dataset Chunks = {config['outputs']['chunks']}")
    # Determine output file name:
    if config['outputs']['format'] == "netcdf":
        extension = "nc"
    else:
        extension = "zarr"
    logging.info(f"* Output File Name = {config['outputs']['output_name']}_YYYY-MM_YYYY-MM.{extension}")
