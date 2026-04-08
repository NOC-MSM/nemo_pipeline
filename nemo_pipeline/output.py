"""
output.py

Description: Output functions for NEMO Pipeline package.

Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
"""

# -- Import Dependencies -- #
import cftime
import dask
import numpy as np
import xarray as xr


# -- Define Utility Functions -- #
def _get_output_filename(
    ds_out: xr.Dataset,
    output_dir: str,
    output_name: str,
    file_format: str,
    date_format: str
    ) -> str:
    """
    Define NEMO Pipeline output filename.

    Parameters:
    -----------
    ds_out : xr.Dataset
        Output xarray Dataset.
    output_dir : str
        Directory to save output file.
    output_name : str
        Prefix of output file name.
    file_format : str
        Output file format. Options are 'netcdf' or 'zarr'.
    date_format : str
        Date format for datetime limits in output filename.
        Options are 'Y' (YYYY), 'M' (YYYY-MM) or 'D' (YYYY-MM-DD).
    """
    # Validate inputs:
    if not isinstance(ds_out, xr.Dataset):
        raise TypeError("ds_out must be an xr.Dataset.")
    if not isinstance(output_dir, str):
        raise TypeError("output_dir must be a string.")
    if not isinstance(output_name, str):
        raise TypeError("output_name must be a string.")
    if file_format not in ["netcdf", "zarr"]:
        raise ValueError("file_format must be either 'netcdf' or 'zarr'.")

    # Define time-limits of output dataset:
    time_limits = ds_out['time_counter'].values[[0, -1]]

    # Create date string from CFTime datetime objects:
    if isinstance(time_limits[0], cftime.datetime):
        if date_format == "Y":
            fmt = "%Y"
        elif date_format == "M":
            fmt = "%Y-%m"
        elif date_format == "D":
            fmt = "%Y-%m-%d"
        else:
            raise ValueError(f"Invalid date_format: '{date_format}'. Options are 'Y', 'M', 'D'.")
        date_str = f"{time_limits[0].strftime(fmt)}-{time_limits[1].strftime(fmt)}"

    # Create date string from numpy datetime64:
    elif isinstance(time_limits[0], np.datetime64):
        date_str = f"{np.datetime_as_string(time_limits[0], unit=date_format)}-{np.datetime_as_string(time_limits[1], unit=date_format)}"
    else:
        raise TypeError(f"Invalid type ({type(time_limits[0])}) for dates. Expected cftime.datetime or np.datetime64.")

    # Define output filename:
    if file_format == "netcdf":
        output_filename = f"{output_dir}/{output_name}_{date_str}.nc"
    elif file_format == "zarr":
        output_filename = f"{output_dir}/{output_name}_{date_str}.zarr"

    return output_filename


def save_nemo_diagnostics(
    ds_out: xr.Dataset,
    output_dir: str,
    output_name: str,
    file_format: str,
    date_format: str,
    chunks: dict | None = None,
    ) -> None:
    """
    Save NEMO Pipeline output dataset to file.

    Parameters:
    -----------
    ds_out : xr.Dataset
        NEMO Pipeline output dataset.
    output_dir : str
        Directory to save output file.
    output_name : str
        Name of output file (without extension).
    file_format : str
        Output file format. Options are 'netcdf' or 'zarr'.
    date_format : str
        Date format for time dimension in output filename.
        Options are 'Y' (YYYY), 'M' (YYYY-MM) or 'D' (YYYY-MM-DD).
    chunks : dict, optional
        Dictionary defining chunk sizes for output dataset.
        Default is None, meaning no chunking is applied.

    Returns:
    --------
    str
        Filepath to saved NEMO Pipeline output file.
    """
    # -- Validate inputs -- #
    if not isinstance(ds_out, xr.Dataset):
        raise TypeError("ds_out must be an xr.Dataset.")
    if chunks is not None and not isinstance(chunks, dict):
        raise TypeError("chunks must be a dictionary.")
    if not isinstance(output_dir, str):
        raise TypeError("output_dir must be a string.")
    if not isinstance(output_name, str):
        raise TypeError("output_name must be a string.")
    if file_format not in ["netcdf", "zarr"]:
        raise ValueError("file_format must be either 'netcdf' or 'zarr'.")
    if date_format not in ["Y", "M", "D"]:
        raise ValueError("date_format must be 'Y', 'M' or 'D'.")

    # Define output filepath:
    output_filepath = _get_output_filename(
        ds_out=ds_out,
        output_dir=output_dir,
        output_name=output_name,
        file_format=file_format,
        date_format=date_format
        )

    # Apply chunking if specified:
    if chunks is not None:
        ds_out = ds_out.chunk(chunks)

    # Write NEMO diagnostic(s) to output file:
    if file_format == "netcdf":
        with dask.config.set(scheduler="synchronous"):
            ds_out.to_netcdf(path=output_filepath, unlimited_dims="time_counter", mode="w")
    elif file_format == "zarr":
        with dask.config.set(scheduler="synchronous"):
            ds_out.to_zarr(store=output_filepath, mode="w")

    return output_filepath
