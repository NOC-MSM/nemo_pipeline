"""
utils.py

Description: Utility functions for NEMO Pipeline package.

Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
"""

# -- Import dependencies -- #
import tomllib
import importlib
from pathlib import Path

from nemo_pipeline.validation import AppConfig


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
    AppConfig
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
