#!/bin/bash

set -euo pipefail
# ================================================================
# run_eORCA025_ERA5v1_OSNAP_pipeline.sh
#
# Description: Run NEMO Pipeline to extract the Overturning in
# the Subpolar North Atlantic Program (OSNAP) hydrographic section
# from the eORCA025 ERA5v1 simulation.
#
# Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
# Created On: 2025-10-28   
# ================================================================

# -- Input arguments to NEMO Pipeline -- #
# Define filepaths:
config_file=example_config.toml
log_file=example_pipeline.log
# Define input file pattern to override config filepaths where {ip} is found:
input_pattern=202

# -- Python Environment -- #
# Activate miniconda environment:
source /home/otooth/miniconda3/etc/profile.d/conda.sh
conda activate /dssgfs01/working/otooth/Software/conda_envs/env_nemo_cookbook

# -- Run NEMO Pipeline CLI -- #
nemo_pipeline run $config_file --log $log_file --input-pattern $input_pattern
