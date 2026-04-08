#!/bin/bash

set -euo pipefail
# ================================================================
# submit_eORCA1_ERA5v1_OSNAP_pipeline.sh
#
# Description: Run NEMO Pipeline to extract the Overturning in
# the Subpolar North Atlantic Program (OSNAP) hydrographic section
# from the eORCA1-ERA5v1 simulation.
#
# Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
# Created On: 2025-11-01   
# ================================================================

# -- Input arguments to NEMO Pipeline -- #
# Define filepaths:
config_file=eORCA12_ERA5v1_BSEA_config.toml
log_file=eORCA12_ERA5v1_BSEA_pipeline.log

# -- Python Environment -- #
# Activate miniconda environment:
source ~/.conda_init
conda activate env_nemo_cookbook

# -- Run NEMO Pipeline CLI -- #
# nemo_pipeline describe $config_file --log $log_file --input-pattern 2004
# nemo_pipeline run $config_file --log $log_file --input-pattern 2004
  nemo_pipeline submit $config_file --log $log_file --submit
