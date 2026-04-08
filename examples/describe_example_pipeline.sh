#!/bin/bash

set -euo pipefail
# ================================================================
# describe_example_pipeline.sh
#
# Description: Run NEMO Pipeline to extract the Overturning in
# the Subpolar North Atlantic Program (OSNAP) hydrographic section
# from an example eORCA1 ERA5v1 simulation.
#
# NEMO Pipeline will be described without execution.
#
# Created By: Ollie Tooth (oliver.tooth@noc.ac.uk) 
# ================================================================

# -- Input arguments to NEMO Pipeline -- #
# Define filepaths:
config_file=example_config.toml
log_file=example_pipeline.log
# Define input file pattern to override config filepaths where {ip} is found:
input_pattern=202

# -- Run NEMO Pipeline CLI -- #
nemo_pipeline describe $config_file --log $log_file --input-pattern $input_pattern
