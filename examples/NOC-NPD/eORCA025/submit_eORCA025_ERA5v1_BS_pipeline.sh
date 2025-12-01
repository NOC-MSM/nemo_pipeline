#!/bin/bash

set -euo pipefail
# ================================================================
# submit_eORCA025_ERA5v1_BS_pipeline.sh
#
# Description: Run NEMO Pipeline to extract the Barents Sea (BS)
# hydrographic section from the eORCA025-ERA5v1 simulation.
#
# Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
# Created On: 2025-12-01
# ================================================================

# -- Input arguments to NEMO Pipeline -- #
# Define filepaths:
config_file=eORCA025_ERA5v1_BS_config.toml
log_file=eORCA025_ERA5v1_BS_pipeline.log

# -- Python Environment -- #
# Activate miniconda environment:
source /home/otooth/miniconda3/etc/profile.d/conda.sh
conda activate /dssgfs01/working/otooth/Software/conda_envs/env_nemo_cookbook

# -- Run NEMO Pipeline CLI -- #
# nemo_pipeline describe $config_file --log $log_file --input-pattern 2025
# nemo_pipeline run $config_file --log $log_file --input-pattern 2025
nemo_pipeline submit $config_file --log $log_file --submit