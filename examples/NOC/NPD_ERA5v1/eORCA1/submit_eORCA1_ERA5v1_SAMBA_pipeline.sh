#!/bin/bash

set -euo pipefail
# ================================================================
# submit_eORCA1_ERA5v1_SAMBA_pipeline.sh
#
# Description: Run NEMO Pipeline to extract the Overturning in
# the South Atlantic Meridional Overturning (SAMBA) hydrographic
# section from the eORCA1-ERA5v1 simulation.
#
# Virtual Environment: env_nemo
#
# Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
# ================================================================

# -- Input arguments to NEMO Pipeline -- #
# Define filepaths:
config_file=eORCA1_ERA5v1_SAMBA_config.toml
log_file=eORCA1_ERA5v1_SAMBA_pipeline.log

# -- Run NEMO Pipeline CLI -- #
# nemo_pipeline describe $config_file --log $log_file --input-pattern 2025
# nemo_pipeline run $config_file --log $log_file --input-pattern 2025
nemo_pipeline submit $config_file --log $log_file --submit