# NEMO Pipeline

**NEMO Pipeline** is a Python library dedicated to building reproducible data pipelines for calculating offline diagnostics using NEMO ocean general circulation model outputs.

### What is a NEMO Pipeline?

Data pipelines are automated systems which open raw input data, calculate one or more diagnostics and write the results to an output destination.

**NEMO Pipeline** enables users to create scalable diagnostic pipelines for their ocean analysis or model validation using the three steps outlined above.

* **Input** --> Opening a collection of NEMO netCDF output files as one or more `xarray.Datasets`.

* **Diagnostics** --> Use the `NEMODataTree` xarray object introduced in the [nemo_cookbook](https://github.com/NOC-MSM/nemo_cookbook/) library to calculate offline diagnostics, such as masked statistics and extracting hydrographic sections.

* **Output** --> Write the diagnostics stored in an `xarray.Dataset` to a local file (netCDF or Zarr).

## Getting Started

### Installation

To get started, download and install **NEMO Pipeline** into a new Python virtual environment via GitHub as follows.

First, clone the **NEMO Pipeline** repository to your local machine:

```bash
git clone git@github.com:NOC-MSM/nemo_pipeline.git
```

Next, activate your new Python virtual environment and install the **NEMO Cookbook** library using `pip`:

```bash
pip install git+https://github.com/NOC-MSM/nemo_cookbook.git
```

Finally, you can install **NEMO Pipeline** into your virtual environment in editable mode:

```bash
pip install -e .
```

**Note:** Here we have assumed that you are located inside your local copy of the `nemo_pipeline` directory.

### NEMO Pipeline CLI

The **NEMO Pipeline** library allows users to create scalable pipelines to produce diagnostics from NEMO ocean model outputs using a **command line interface (CLI)**.

All diagnostic pipelines are defined using a config `.ini` file.

There are three commands available to users:

* `describe` --> Summarise & validate the steps of a pipeline.

* `run` --> Run pipeline in the current Python process.

* `submit` --> Submit pipeline to be executed via a SLURM job array.

Each command can be used with the following syntax:

```bash
nemo_pipeline COMMAND [ARGS]
```

where `COMMAND` corresponds to the chosen command and `[ARGS]` represents the required and optional arguments used to define a pipeline. See below for details.

### Config File

To define a new **NEMO Pipeline**, we must populate a config `.toml` file which is structured into four sections as follows:

* `[slurm]` --> Define parameters used to create SLURM job array script for submission.

* `[inputs]` --> Define NEMO ocean model domain and grid filepaths and variables to extract.

* `[diagnostics]` --> Define name(s) of diagnostic functions which ingest `NEMODataTree` objects and return `xarray.Datasets`.

* `[outputs]` --> Define output filepaths, naming conventions and chunk sizes.

Below we present an example template for a config `.toml` file for extracting the Overturning in the Subpolar North Atlantic Program (OSNAP) array from a typical NEMO model output dataset.

We use SLURM to divide the pipeline into 10 tasks (1 per year, 1990-1999), which will be submitted as a job array with a maximum of 2 tasks being executed concurrently.

```bash
[slurm]
# Define directories for SLURM job scripts and logs:
job_dir = "/dssgfs01/working/otooth/Software/nemo_pipeline/examples/outputs/jobs"
log_dir = "/dssgfs01/working/otooth/Software/nemo_pipeline/examples/outputs/logs"
log_prefix = "eORCA025_ERA5_OSNAP"
# SLURM batch job submission parameters:
sbatch_job_name = "nemo_pipeline_osnap"
sbatch_time = "00:10:00"
sbatch_partition = "test"
sbatch_ntasks = 1
sbatch_mem = "6G"
# Define the initial, final and step input patterns for batch job submission:
ip_start = 2010
ip_end = 2014
ip_step = 1
# Define maximum number of concurrent SLURM jobs:
max_concurrent_jobs = 2
# Define Python virtual environment activation command:
venv_cmd = "source /home/otooth/miniconda3/etc/profile.d/conda.sh; conda activate /dssgfs01/working/otooth/Software/conda_envs/env_nemo_cookbook"

[inputs]
# Define NEMO ocean model filepaths used to construct NEMODataTree object:
nemo_dir = "/dssgfs01/scratch/npd/simulations/eORCA025_ERA5_v1"
domain_filepath = "/dssgfs01/scratch/npd/simulations/Domains/eORCA025/domain_cfg.nc"
# Create NEMO model grid datasets from CMORISED variables:
cmorised = false
# Domain Properties:
iperio = true
nftype = "T"
read_mask = false
# NEMO T-grid (scalar) variables:
gridT_filepath = "${nemo_dir}/eORCA025_ERA5_1m_grid_T_{ip}*.nc"
gridT_vars = [ "thetao_con", "so_abs" ]
# NEMO U-grid (zonal vector) variables:
gridU_filepath = "${nemo_dir}/eORCA025_ERA5_1m_grid_U_{ip}*.nc"
gridU_vars = [ "uo", "e3u" ]
# NEMO V-grid (meridional vector) variables:
gridV_filepath = "${nemo_dir}/eORCA025_ERA5_1m_grid_V_{ip}*.nc"
gridV_vars = [ "vo", "e3v" ]
# NEMO W-grid (vertical vector) variables:
 gridW_filepath = "/path/to/eORCA_grid_W_{ip}*.nc"
gridW_vars = [ "my", "vars" ]
# NEMO icemod (sea-ice) variables:
icemod_filepath = "/path/to/eORCA_icemod_{ip}*.nc"
icemod_vars = [ "my", "vars"]

[diagnostics]
# Define diagnostics to be computed using NEMODataTree:
diagnostic_name = "extract_osnap_section"

[outputs]
# Define NEMO ocean model pipeline output file:
output_dir = "/dssgfs01/working/otooth/Software/nemo_pipeline/examples/outputs"
output_name = "eORCA025_ERA5_v1_OSNAP"
date_format = "M"
format = "netcdf"
# Define output dataset chunking:
chunks = { time_counter = 1, k = 75 }
```

### CLI Arguments

| Long version | Optional | Description |
|---|---|---|
| `COMMAND` | **No** | Specify the action: `describe` / `run` / `submit`. |
| `config` | **No** | Path to NEMO pipeline config .toml file |
| `--log` | **Yes** | Path to write NEMO pipeline log file. |
| `--input-pattern` | **Yes** | Pattern used to subsititute `{ip}` in NEMO model input file paths in config file. |
| `--submit` / `--no-submit` | **Yes** | Submit the job to the SLURM scheduler. |


## Contact

Ollie Tooth (**oliver.tooth@noc.ac.uk**) 