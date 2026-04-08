"""
pipeline.py

Description: Pipeline functions for NEMO Pipeline package.

Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
"""

# -- Import Dependencies -- #
import logging

from nemo_pipeline.input import (
    _create_chunk_dicts,
    _create_grid_filepaths,
    _create_variable_lists,
    create_nemodatatree,
    open_cmorised_datasets,
    open_nemo_datasets,
)
from nemo_pipeline.output import save_nemo_diagnostics
from nemo_pipeline.utils import load_config, load_diagnostic


# -- Define Pipeline Functions -- #
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
    ds_diag = diag_func(nemo=nemo, **config['diagnostics']['kwargs'])
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

    # Close all files associated with NEMODataTree:
    nemo.close()
    logging.info("Completed: Closed all netcdf files associated with NEMODataTree.")


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
    grid_filepaths = _create_grid_filepaths(config=config, args=args)
    grid_variables = _create_variable_lists(config=config)
    grid_chunks = _create_chunk_dicts(config=config)
    for grid in grid_filepaths:
        filepath = grid_filepaths[grid]
        var_names = grid_variables[grid]
        if filepath is not None:
            if config['inputs']['cmorised']:
                logging.info(f"* Create NEMO model grid dataset from CMORISED variables {var_names} --> {filepath} with chunks {grid_chunks[grid]}")
            else:
                logging.info(f"* Open {var_names} from NEMO model grid dataset --> {filepath} with chunks {grid_chunks[grid]}")

    # NEMODataTree:
    logging.info("Create NEMODataTree from NEMO datasets using:")
    logging.info(f"* iperio = {config['inputs']['iperio']}")
    logging.info(f"* nftype = {config['inputs']['nftype']}")
    logging.info(f"* read_mask = {config['inputs']['read_mask']}")

    logging.info("==== Diagnostics ====")
    d_diag = config['diagnostics']['diagnostic']
    logging.info(f"Calculate NEMO offline diagnostic --> {d_diag['function']}({config['diagnostics']['kwargs']})")

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
