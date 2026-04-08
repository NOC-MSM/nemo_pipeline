"""
NEMO Pipeline

A Python library for building reproducible data pipelines to calculate diagnostics using NEMO
ocean general circulation model outputs at scale.
"""
__author__ = "Ollie Tooth (oliver.tooth@noc.ac.uk)"
__credits__ = "National Oceanography Centre (NOC), Southampton, UK"

from importlib.metadata import version as _version

from nemo_pipeline import (
    cli,
    input,
    output,
    pipeline,
    submit,
    utils,
    validation,
    diagnostics
)

try:
    __version__ = _version("nemo_pipeline")
except Exception:
    # Local copy or not installed with setuptools.
    # Disable minimum version checks on downstream libraries.
    __version__ = "9999.0.0"

__all__ = ("cli", "input", "output", "pipeline", "submit", "utils", "validation", "diagnostics")