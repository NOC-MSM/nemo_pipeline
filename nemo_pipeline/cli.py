"""
cli.py

Description: Main command line interface for NEMO Pipeline package.

Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
Date Created: 28/10/2025
"""

# -- Import dependencies -- #
import sys
import logging
import argparse
import nemo_pipeline.diagnostics as nemo_diags

from .__init__ import __version__
from nemo_pipeline.utils import get_config, parse_chunks
from nemo_pipeline.pipeline import open_nemo_datasets, create_nemodatatree, save_nemo_diagnostics, describe_nemo_pipeline

logger = logging.getLogger(__name__)

# -- Define CLI Functions -- #
def create_header() -> None:
    """
    Add NEMO Pipeline header to log.
    """
    logger.info(
        f"""
   ⦿──⦿──⦿──⦿──⦿──⦿──⦿──⦿──⦿
         NEMO Pipeline      
   ⦿──⦿──⦿──⦿──⦿──⦿──⦿──⦿──⦿ 

        version: {__version__}

""",
        extra={"simple": True},
    )


def init_logging(log_filepath: str) -> None:
    """
    Initialise NEMO Pipeline logging.

    Parameters:
    -----------
    log_filepath : str | None
        Filepath to log file. If None, logs to 'nemo_pipeline.log'.
    """
    # Verify input:
    if not isinstance(log_filepath, str):
        raise TypeError("log_filepath must be a string.")

    logging.basicConfig(
        format="⦿──⦿  NEMO Pipeline  ⦿──⦿  | %(levelname)10s | %(asctime)s | %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_filepath),
            logging.StreamHandler()
    ]
    )


def create_argparser() -> argparse.ArgumentParser:
    """
    Create the argument parser.
    """
    # Create argument parser:
    parser = argparse.ArgumentParser(
        description=f"NEMO Pipeline {__version__} command line interface",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Add NEMO Pipeline CLI actions:
    parser.add_argument(
        "action",
        choices=["describe", "run"],
        help="Specify NEMO Pipeline action: 'run' to execute pipeline or 'describe' to summarise stages of pipeline defined using config file",
    )

    # Add NEMO Pipeline CLI required arguments:
    parser.add_argument('-c', '--config', type=str, action='store', dest='config_file',
                        required=True, help='Path to the NEMO Pipeline config .ini file.')
    
    # Add NEMO Pipeline CLI optional arguments:
    parser.add_argument('-l', '--log_path', type=str, action='store', dest='log_filepath',
                        default='nemo_pipeline.log', help='Path to write NEMO Pipeline log file.')

    return parser


def run_nemo_pipeline(args: dict) -> None:
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
    # Read config file:
    config = get_config(args=args)
    logging.info(f"Completed: Read & verified config file -> {args['config_file']}")

    # Open NEMO model domain & grid datasets:
    logging.info("In Progress: Reading NEMO model domain & grid datasets...")
    d_nemo = open_nemo_datasets(config=config)
    logging.info("Completed: Reading NEMO model domain & grid datasets.")

    # Create NEMODataTree object:
    logging.info("In Progress: Constructing NEMODataTree from NEMO datasets...")
    nemo = create_nemodatatree(d_nemo=d_nemo,
                               iperio=config.getboolean("INPUTS", "iperio"),
                               nftype=config.get("INPUTS", "nftype"),
                               read_mask=config.getboolean("INPUTS", "read_mask")
                               )
    logging.info("Completed: Constructed NEMODataTree from NEMO datasets.")

    # === Diagnostics === #
    logging.info("==== Diagnostics ====")
    # Calculate specified NEMO offline diagnostic(s):
    diag_name = config.get("DIAGNOSTICS", "diagnostic_name")
    diag_func = getattr(nemo_diags, diag_name)
    logging.info(f"In Progress: Calculating NEMO offline diagnostic -> {diag_name}...")
    ds_diag = diag_func(nemo=nemo)
    logging.info(f"Completed: Calculated NEMO offline diagnostic -> {diag_name}.")

    # === Outputs === #
    logging.info("==== Outputs ====")
    logging.info(f"In Progress: Saving NEMO diagnostic(s) to {config.get('OUTPUTS', 'format')} file...")
    # Parse chunking str to dict:
    chunks = parse_chunks(chunks_str=config.get("OUTPUTS", "chunks"))

    # Write NEMO Pipeline output dataset to file:
    output_filepath = save_nemo_diagnostics(
        ds_out=ds_diag,
        output_dir=config.get("OUTPUTS", "output_dir"),
        output_name=config.get("OUTPUTS", "output_name"),
        file_format=config.get("OUTPUTS", "format"),
        date_format=config.get("OUTPUTS", "date_format"),
        chunks=chunks
        )

    logging.info(f"Completed: Saved NEMO diagnostic(s) to file -> {output_filepath}")


def nemo_pipeline() -> None:
    """
    Run the NEMO Pipeline command line interface.
    """
    # -- Parse Command Line Arguments -- #
    parser = create_argparser()
    args = vars(parser.parse_args())

    if len(sys.argv) == 1:
        args.parser.print_help()
        sys.exit(0)

    # -- Initialise Logging -- #
    init_logging(log_filepath=args['log_filepath'])
    create_header()

    # -- Perform NEMO Pipeline action -- #
    if args['action'] == 'describe':
        describe_nemo_pipeline(args)

    elif args['action'] == 'run':
        run_nemo_pipeline(args)
    
    logging.info("✔ NEMO Pipeline Completed ✔")
    sys.exit(0)
