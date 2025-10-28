"""
diagnostics.py

Description: Diagnostic functions for NEMO Pipeline.

Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
Date Created: 28/10/2025
"""

# -- Import dependencies -- #
import xarray as xr
from nemo_cookbook import NEMODataTree


# -- NEMO Offline Diagnostics -- #
def extract_osnap_section(
    nemo: NEMODataTree,
    ) -> xr.Dataset:
    """
    Extract Overturning in the Subpolar North Atlantic Program
    (OSNAP) hydrographic section from NEMODataTree.

    Parameters:
    -----------
    nemo : NEMODataTree
        A hierarchical data tree of NEMO model outputs.

    Returns:
    --------
    xr.Dataset
        Velocity, conservative temperature and absolute salinity data
        extracted along the OSNAP section.
    """
    # Validate inputs:
    if not isinstance(nemo, NEMODataTree):
        raise TypeError("nemo must be a NEMODataTree object.")

    # Open OSNAP coords from gridded observations dataset:
    ds_osnap = xr.open_zarr("https://noc-msm-o.s3-ext.jc.rl.ac.uk/ocean-obs/OSNAP/OSNAP_Gridded_TSV_201408_202006_2023")
    lon_osnap = ds_osnap['LONGITUDE'].values
    lat_osnap = ds_osnap['LATITUDE'].values

    # Extract section from parent domain of NEMODataTree:
    ds_bdy = nemo.extract_section(
        lon_section=lon_osnap,
        lat_section=lat_osnap,
        uv_vars=['uo', 'vo'],
        vars=['thetao_con', 'so_abs'],
        dom='.',
        )

    return ds_bdy