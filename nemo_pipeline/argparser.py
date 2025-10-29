"""
argparser.py

Description: Argument parser for NEMO Pipeline package.

Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
Date Created: 29/10/2025
"""

# -- Import dependencies -- #
import argparse
from .__init__ import __version__

# -- Argument Parser -- #
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
        choices=["describe", "run", "submit"],
        help="Specify NEMO Pipeline action: 'run' to execute pipeline, 'submit' to submit pipeline as SLURM job array, 'describe' to summarise stages of pipeline defined using config file",
    )

    # Add NEMO Pipeline CLI required arguments:
    parser.add_argument('-c', '--config', type=str, action='store', dest='config_file',
                        required=True, help='Path to the NEMO Pipeline config .ini file.')
    
    # Add NEMO Pipeline CLI optional arguments:
    parser.add_argument('-l', '--log', type=str, action='store', dest='log_filepath',
                        default='nemo_pipeline.log', help='Path to write NEMO Pipeline log file.')

    parser.add_argument('-i', '--input_pattern', type=str, action='store', dest='input_pattern',
                        default=None, help='Pattern used to subsititute {ip} in NEMO model input file paths in config file.')

    parser.add_argument('-ns', '--no_submit', action='store_true', dest='no_submit',
                        help='Do not submit the job to the SLURM scheduler. Useful to generate the job script only.')

    return parser
