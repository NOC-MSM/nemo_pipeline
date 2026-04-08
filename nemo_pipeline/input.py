"""
input.py

Description: Input functions for NEMO Pipeline package.

Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
"""

# -- Import Dependencies -- #
import glob
import logging
import numpy as np
import xarray as xr
from nemo_cookbook import NEMODataTree


# -- Define Utility Functions -- #
def _create_grid_filepaths(
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
    # Define NEMO model grid filepaths from config:
    inputs = config["inputs"]
    grid_filepaths = {}

    # -- Collect filepaths for each NEMO model grid -- #
    for grid in ['gridT', 'gridU', 'gridV', 'gridW', 'gridF', 'icemod']:
        filepath = inputs[grid].get("filepath", None)

        # Replace input pattern in config filepaths:
        if filepath is not None:
            if inputs['cmorised']:
                # List of filepaths for CMORISED variables per NEMO model grid:
                if not isinstance(filepath, list):
                    raise RuntimeError(f"Expected list of filepaths for CMORISED variables, received {type(filepath)}")
                for n, fpath in enumerate(filepath):
                    if '${nemo_dir}' in fpath:
                        fpath = fpath.replace('${nemo_dir}', inputs['nemo_dir'])
                    if '{ip}' in fpath:
                        if args['input_pattern'] == "":
                            raise ValueError(f"Missing --input_pattern argument to replace {{ip}} in {grid} filepath[{n}].")
                        fpath = fpath.replace('{ip}', args['input_pattern'])
                        logging.info(f"* Overriding {grid}_filepath[{n}] using input pattern --> {fpath}")
                    filepath[n] = fpath
            else:
                # Individual filepath per NEMO model grid:
                if not isinstance(filepath, str):
                    raise RuntimeError(f"Expected string filepath for NEMO model grid, received {type(filepath)}")
                if '${nemo_dir}' in filepath:
                    filepath = filepath.replace('${nemo_dir}', inputs['nemo_dir'])
                if '{ip}' in filepath:
                    if args['input_pattern'] == "":
                        raise ValueError(f"Missing --input_pattern argument to replace {{ip}} in {grid} filepath.")
                    filepath = filepath.replace('{ip}', args['input_pattern'])
                    logging.info(f"* Overriding {grid}_filepath using input pattern --> {filepath}")

        # Add grid filepath:
        grid_filepaths[grid] = filepath

    return grid_filepaths


def _create_variable_lists(
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
    # -- Collect NEMO model grid variables from config -- #
    inputs = config["inputs"]
    grid_variables = {}
    for grid in ['gridT', 'gridU', 'gridV', 'gridW', 'gridF', 'icemod']:
        grid_variables[grid] = inputs[grid].get("vars", None)

    return grid_variables


def _create_chunk_dicts(
    config: dict
    ) -> dict[str, dict[str, int]]:
    """
    Create dictionary of NEMO model grid chunk dictionaries from config.

    Parameters:
    -----------
    config : dict
        Configuration parameters, including NEMO model output variables.

    Returns:
    --------
    dict[str, dict[str, int]]
        Dictionary of NEMO model grid chunk dictionaries.
    """
    # -- Collect NEMO model grid chunk dictionaries from config -- #
    inputs = config["inputs"]
    grid_chunks = {}
    for grid in ['gridT', 'gridU', 'gridV', 'gridW', 'gridF', 'icemod']:
        grid_chunks[grid] = inputs[grid].get("chunks", {})

    return grid_chunks


def _collect_filepaths(
    filepath: str
    ) -> list[str]:
    """
    Collect list of filepaths matching a filepath pattern.
    Parameters:
    -----------
    filepath : str
        Filepath pattern containing wildcard or input pattern expression(s).

    Returns:
    --------
    list[str]
        List of filepaths matching the input pattern / wildcard(s).
    """
    # -- Validate Input -- #
    if not isinstance(filepath, str):
        raise TypeError("filepath must be a string.")

    # -- Collect complete filepaths by expanding patterns & wildcards -- #
    if ("[" in filepath) and ("]" in filepath):
        substrings = filepath.split("[")[1].split("]")[0]
        filepaths = []
        for substr in substrings.replace(" ", "").split(","):
            filepaths.extend(glob.glob(filepath.replace(f"[{substrings}]", substr)))
    else:
        filepaths = glob.glob(filepath)

    if len(filepaths) == 0:
        raise FileNotFoundError(f"No files found matching filepath: {filepath}")

    return filepaths


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
    # Add dimension coordinates:
    ds_domain = ds_domain.assign_coords({'nav_lev': np.arange(ds_domain['nav_lev'].size),
                                         'y': np.arange(ds_domain['y'].size),
                                         'x': np.arange(ds_domain['x'].size)
                                        })

    return ds_domain.squeeze()


def open_grid_ds(
    filepath: str,
    variables: list[str] | None = None,
    chunks: dict[str, int] = {}
    ) -> xr.Dataset:
    """
    Open NEMO model grid dataset from a netCDF file(s).

    Parameters:
    -----------
    filepath : str
        Filepath pattern to NEMO model grid netCDF file(s).
    variables : list of str, optional
        List of variable names to load from the dataset. If None, all variables are loaded.
    chunks : dict[str, int], optional
        Dictionary defining chunk sizes for loading the dataset. Default is an empty dictionary,
        meaning chunks are inferred from the input netCDF file.

    Returns:
    --------
    xr.Dataset
        NEMO model grid dataset.
    """
    # -- Validate Inputs -- #
    if not isinstance(filepath, str):
        raise TypeError("filepath must be a string.")
    if not isinstance(variables, list) and variables is not None:
        raise TypeError("variables must be a list of strings or None.")
    if not isinstance(chunks, dict):
        raise TypeError("chunks must be a dictionary.")

    # -- Open NEMO model grid dataset with specified variables -- #
    filepaths = _collect_filepaths(filepath)

    # Define CFDatetimeCoder to decode time coords:
    coder = xr.coders.CFDatetimeCoder(time_unit="s")

    # Open NEMO model grid dataset with specified variables only:
    if len(filepaths) == 1:
        try:
            if variables is None:
                ds_grid = xr.open_dataset(filepaths[0], decode_times=coder, engine="netcdf4", chunks=chunks)
            else:
                ds_grid = xr.open_dataset(filepaths[0], decode_times=coder, engine="netcdf4", chunks=chunks)[variables]
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
                                            engine="netcdf4",
                                            chunks=chunks
                                            )
            else:
                ds_grid = xr.open_mfdataset(filepaths,
                                            data_vars="minimal",
                                            compat="no_conflicts",
                                            decode_times=coder,
                                            parallel=False,
                                            engine="netcdf4",
                                            chunks=chunks,
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
    # -- Verify Input -- #
    if not isinstance(config, dict):
        raise TypeError("config must be a dictionary.")
    if not isinstance(args, dict):
        raise TypeError("args must be a dictionary.")

    # -- Open NEMO domain configuration -- #
    inputs = config["inputs"]
    domain_filepath = inputs.get("domain_filepath", None)
    if domain_filepath is None:
        raise ValueError("domain_filepath must be specified in the config file.")
    else:
        d_nemo = {}
        d_nemo["domain"] = open_domain_ds(domain_filepath)
        logging.info("--> Completed: Opened NEMO model domain_cfg dataset")

    # -- Open NEMO model grid datasets -- #
    grid_filepaths = _create_grid_filepaths(config=config, args=args)
    grid_variables = _create_variable_lists(config=config)
    grid_chunks = _create_chunk_dicts(config=config)

    for grid in grid_filepaths:
        filepath = grid_filepaths[grid]
        var_names = grid_variables[grid]
        if filepath is not None:
            # Open grid dataset with specified variables:
            d_nemo[grid] = open_grid_ds(filepath=filepath,
                                        variables=var_names,
                                        chunks=grid_chunks[grid]
                                        )
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
    # -- Verify Input -- #
    if not isinstance(config, dict):
        raise TypeError("config must be a dictionary.")
    if not isinstance(args, dict):
        raise TypeError("args must be a dictionary.")

    # -- Open NEMO domain configuration -- #
    inputs = config["inputs"]
    domain_filepath = inputs.get("domain_filepath", None)
    if domain_filepath is None:
        raise ValueError("domain_filepath must be specified in the config file.")
    else:
        d_nemo = {}
        d_nemo["domain"] = open_domain_ds(filepath=domain_filepath)
        logging.info("--> Completed: Opened NEMO model domain_cfg dataset")

    # -- Open NEMO model grid datasets -- #
    grid_filepaths = _create_grid_filepaths(config=config, args=args)
    grid_variables = _create_variable_lists(config=config)
    grid_chunks = _create_chunk_dicts(config=config)

    logging.info("In Progress: Creating NEMO model grid datasets from CMORISED variables.")
    for grid in grid_filepaths:
        grid_suffix = grid[-1].lower()
        filepaths = grid_filepaths[grid]
        var_names = grid_variables[grid]
        if filepaths is not None:
            # Open each CMORISED variable & merge into single NEMO model grid dataset:
            if len(filepaths) != len(var_names):
                raise ValueError(f"Number of filepaths ({len(filepaths)}) does not equal number of variables ({len(var_names)}) for grid {grid}.")
            for n, fpath in enumerate(filepaths):
                if n == 0:
                    d_nemo[grid] = open_grid_ds(filepath=fpath,
                                                variables=var_names[n],
                                                chunks=grid_chunks[grid]
                                                )
                else:
                    try:
                        d_nemo[grid][var_names[n]] = open_grid_ds(filepath=fpath,
                                                                  variables=var_names[n],
                                                                  chunks=grid_chunks[grid]
                                                                  ).to_dataarray().squeeze()
                    except Exception as e:
                        raise RuntimeError(f"Failed to merge variable {var_names[n]} into NEMO model {grid} dataset: {e}")

            # Update CMORISED variable names to NEMO standard names:
            if "thkcello" in list(d_nemo[grid].data_vars):
                d_nemo[grid] = d_nemo[grid].rename({"thkcello": f"e3{grid_suffix}"})
            # Update CMORISED dimensions to NEMO standard names:
            update_dims = {"i": "x", "j": "y", "time": "time_counter"}
            if "lev" in d_nemo[grid].dims:
                update_dims["lev"] = f"depth{grid_suffix}"
            try:
                d_nemo[grid] = d_nemo[grid].rename(update_dims)
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
    # -- Validate Input -- #
    if not isinstance(d_nemo, dict) & all(isinstance(ds, xr.Dataset) for ds in d_nemo.values()):
        raise TypeError("d_nemo must be a dictionary of xr.Dataset objects.")

    # -- Create NEMODataTree -- #
    datasets = {"parent": d_nemo}
    nemo = NEMODataTree.from_datasets(datasets=datasets,
                                      iperio=iperio,
                                      nftype=nftype,
                                      read_mask=read_mask
                                      )

    return nemo
