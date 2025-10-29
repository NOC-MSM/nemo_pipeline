# NEMO Pipeline

**NEMO Pipeline** is a simple Python library to help scientists & ocean modellers quickly build reproducible data pipelines for calculating offline diagnostics using NEMO ocean general circulation model outputs.

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
nemo_pipeline action --flags
```

where `action` corresponds to the chosen command and `--flags` represent the mandatory and optional arguments used to define a pipeline. See below for details.

### Config File

To define a new **NEMO Pipeline**, we must populate a config `.ini` file which is structured into four sections as follows:

* `[SLURM]` --> Define parameters used to create SLURM job array script for submission.

* `[INPUTS]` --> Define NEMO ocean model domain and grid filepaths and variables to extract.

* `[DIAGNOSTICS]` --> Define name(s) of diagnostic functions which ingest `NEMODataTree` objects and return `xarray.Datasets`.

* `[OUTPUTS]` --> Define output filepaths, naming conventions and chunk sizes.

Below we present an example template for a config `.ini` file for extracting the Overturning in the Subpolar North Atlantic Program (OSNAP) array from a typical NEMO model output dataset.

We use SLURM to divide the pipeline into 10 tasks (1 per year, 1990-1999), which will be submitted as a job array with a maximum of 2 tasks being executed concurrently.

```bash
[SLURM]
# Define directories for SLURM job scripts and logs.
job_dir = path/to/my/jobs
log_dir = path/to/my/logs
log_prefix = my_model
# SLURM batch job submission parameters.
sbatch.job_name = nemo_pipeline
sbatch.time = 00:10:00
sbatch.partition = test
sbatch.ntasks = 1
sbatch.mem = 6G
# Define the initial, final and step input patterns for batch job submission.
ip_start = 1990
ip_end = 1999
ip_step = 1
# Define maximum number of concurrent SLURM jobs.
max_concurrent_jobs = 2
# Define Python virtual environment activation command.
source /my/virtual/environment/bin/activate

[INPUTS]
# Define NEMO ocean model filepaths used to construct NEMODataTree object.
nemo_dir = /path/to/my/nemo/output/
domain_filepath = /path/to/my/domain_cfg.nc
# Domain Properties:
iperio = True
nftype = T
read_mask = False
# NEMO T-grid (scalar) variables:
gridT_filepath = %(nemo_dir)s/eORCA_1m_grid_T_{ip}*.nc
gridT_vars = thetao_con, so_abs
# NEMO U-grid (zonal - vector) variables:
gridU_filepath = %(nemo_dir)s/eORCA_1m_grid_T_{ip}*.nc
gridU_vars = uo, e3u
# NEMO V-grid (meridional - vector) variables:
gridV_filepath = %(nemo_dir)s/eORCA_1m_grid_T_{ip}*.nc
gridV_vars = vo, e3v

[DIAGNOSTICS]
# Define diagnostics to be computed using NEMODataTree.
diagnostic_name = extract_osnap_section

[OUTPUTS]
# Define NEMO ocean model pipeline output file.
output_dir = /path/to/my/outputs
output_name = eORCA_1M_
format = netcdf
chunks = time_counter:1, k:75
date_format = M
```

### CLI Arguments

| Long version | Short Version | Description |
|---|---|---|
| action | | Specify the action: `describe` / `run` / `submit`. |
| `--config` | `-c` | Path to the NEMO Pipeline config .ini file. |
| `--log` | `-l` | Path to write NEMO Pipeline log file. |
| `--input_pattern` | `-i` | Pattern used to subsititute {ip} in NEMO model input file paths in config file. |
| `--no_submit` | `-ns` | Do not submit the job to the SLURM scheduler. Useful to generate the job script only. |


## Contact

Ollie Tooth (oliver.tooth@noc.ac.uk)