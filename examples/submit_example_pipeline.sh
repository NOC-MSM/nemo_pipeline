#!/bin/bash

set -euo pipefail
# ================================================================
# submit_example_pipeline.sh
#
# Description: Run NEMO Pipeline to extract the Overturning in
# the Subpolar North Atlantic Program (OSNAP) hydrographic section
# from an example eORCA1 ERA5v1 simulation.
#
# NEMO Pipeline will be submitted as a job to the SLURM scheduler.
#
# Created By: Ollie Tooth (oliver.tooth@noc.ac.uk) 
# ================================================================

# -- Input arguments to NEMO Pipeline -- #
# Define filepaths:
config_file=example_config.toml
log_file=example_pipeline.log

# -- Run NEMO Pipeline CLI -- #
nemo_pipeline submit $config_file --log $log_file --submit
