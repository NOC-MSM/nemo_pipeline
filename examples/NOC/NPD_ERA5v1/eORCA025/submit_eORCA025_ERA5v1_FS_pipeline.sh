#!/bin/bash

set -euo pipefail
# ================================================================
# submit_eORCA025_ERA5v1_FS_pipeline.sh
#
# Description: Run NEMO Pipeline to extract the Fram Strait (FS)
# hydrographic section from the eORCA025-ERA5v1 simulation.
#
# Virtual Environment: env_nemo
#
# Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
# ================================================================

# -- Input arguments to NEMO Pipeline -- #
# Define filepaths:
config_file=eORCA025_ERA5v1_FS_config.toml
log_file=eORCA025_ERA5v1_FS_pipeline.log

# -- Run NEMO Pipeline CLI -- #
# nemo_pipeline describe $config_file --log $log_file --input-pattern 2025
# nemo_pipeline run $config_file --log $log_file --input-pattern 2025
nemo_pipeline submit $config_file --log $log_file --submit