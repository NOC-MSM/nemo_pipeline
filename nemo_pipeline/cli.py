"""
cli.py

Description: Main command line interface for NEMO Pipeline package.

Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
Date Created: 28/10/2025
"""

# -- Import dependencies -- #
import sys
import logging
from .__init__ import __version__
from .argparser import create_argparser
from nemo_pipeline.submit import submit_slurm_pipeline
from nemo_pipeline.pipeline import describe_nemo_pipeline, run_nemo_pipeline

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


def init_logging(
    log_filepath: str
    ) -> None:
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
        logging.info("✔ NEMO Pipeline Completed ✔")

    elif args['action'] == 'run':
        run_nemo_pipeline(args)
        logging.info("✔ NEMO Pipeline Completed ✔")
    
    elif args['action'] == 'submit':
        submit_slurm_pipeline(args)
        logging.info("✔ NEMO Pipeline Submitted ✔")

    else:
        raise ValueError(f"Invalid action: {args['action']}. Options are: 'describe', 'run', 'submit'.")

    sys.exit(0)
