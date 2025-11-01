"""
cli.py

Description: Main command line interface for NEMO Pipeline package.

Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
Date Created: 28/10/2025
"""

# -- Import dependencies -- #
import typer
import logging
from .__init__ import __version__
from typing_extensions import Annotated
from nemo_pipeline.submit import submit_slurm_pipeline
from nemo_pipeline.pipeline import describe_nemo_pipeline, run_nemo_pipeline

app = typer.Typer()
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


# -- Create Typer App -- #
@app.command()
def describe(
    config: Annotated[str, typer.Argument(help="Path to NEMO Pipeline config .toml file")],
    log: Annotated[
        str,
        typer.Option(help="Path to write NEMO Pipeline log file", rich_help_panel="Options")
    ] = "nemo_pipeline.log",
    input_pattern: Annotated[
        str,
        typer.Option(help="Pattern used to substitute {ip} in NEMO grid filepaths in config .toml file.", rich_help_panel="Options"),
    ] = "",
) -> None:
    """
    Describe NEMO pipeline defined by configuration (.toml) file.
    """
    # -- Initialise Logging -- #
    init_logging(log_filepath=log)
    create_header()

    # -- Describe NEMO Pipeline -- #
    args = {
        "config_file": config,
        "log_filepath": log,
        "input_pattern": input_pattern,
    }
    describe_nemo_pipeline(args=args)
    logging.info("✔ NEMO Pipeline Completed ✔")


@app.command()
def run(
    config: Annotated[str, typer.Argument(help="Path to NEMO Pipeline config .toml file")],
    log: Annotated[
        str,
        typer.Option(help="Path to write NEMO Pipeline log file", rich_help_panel="Options")
    ] = "nemo_pipeline.log",
    input_pattern: Annotated[
        str,
        typer.Option(help="Pattern used to substitute {ip} in NEMO grid filepaths in config .toml file.", rich_help_panel="Options"),
    ] = "",
) -> None:
    """
    Run NEMO pipeline defined by configuration (.toml) file in current process.
    """
    # -- Initialise Logging -- #
    init_logging(log_filepath=log)
    create_header()

    # -- Run NEMO Pipeline -- #
    args = {
        "config_file": config,
        "log_filepath": log,
        "input_pattern": input_pattern,
    }
    run_nemo_pipeline(args=args)
    logging.info("✔ NEMO Pipeline Completed ✔")


@app.command()
def submit(
    config: Annotated[str, typer.Argument(help="Path to NEMO Pipeline config .toml file")],
    log: Annotated[
        str,
        typer.Option(help="Path to write NEMO Pipeline log file", rich_help_panel="Options")
    ] = "nemo_pipeline.log",
    submit: Annotated[
        bool,
        typer.Option(help="Submit the job to the SLURM scheduler.", rich_help_panel="Options"),
    ] = True,
) -> None:
    """
    Submit NEMO pipeline defined by configuration (.toml) file as a SLURM job array.
    """
    # -- Initialise Logging -- #
    init_logging(log_filepath=log)
    create_header()

    # -- Submit NEMO Pipeline -- #
    args = {
        "config_file": config,
        "log_filepath": log,
        "submit": submit,
    }
    submit_slurm_pipeline(args=args)
    if submit:
        logging.info("✔ NEMO Pipeline Submitted ✔")
    else:
        logging.info("✔ NEMO Pipeline Completed ✔")


if __name__ == "__main__":
    app()
