#!/bin/bash

set -euo pipefail
# ================================================================
# submit_eORCA025_ERA5v1_OSNAP_pipeline.sh
#
# Description: Submit NEMO Pipeline to extract the Overturning in
# the Subpolar North Atlantic Program (OSNAP) hydrographic section
# from the eORCA025 ERA5v1 simulation as SLURM job array.
#
# Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
# Created On: 2025-10-29
# ================================================================

# -- Input arguments to NEMO Pipeline -- #
# Initial & final years of eORCA025 output files:
config_file=example_OSNAP_config.ini
log_file=example_OSNAP.log

# -- Python Environment -- #
# Activate miniconda environment:
source /home/otooth/miniconda3/etc/profile.d/conda.sh
conda activate /dssgfs01/working/otooth/Software/conda_envs/env_nemo_cookbook

# -- Run NEMO Pipeline CLI -- #
nemo_pipeline submit -c $config_file -l $log_file
