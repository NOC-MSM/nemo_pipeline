"""
usrdef.py

Description: User-defined diagnostic functions for NEMO Pipeline.

Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
Date Created: 03/11/2025
"""

# -- Import dependencies -- #
import numpy as np
import xarray as xr
from nemo_cookbook import NEMODataTree


# -- User-defined Diagnostics -- #
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
    # Define OSNAP section coordinates (adding final land point - UK):
    lon_osnap = np.concatenate([ds_osnap['LONGITUDE'].values, np.array([-4.0])])
    lat_osnap = np.concatenate([ds_osnap['LATITUDE'].values, np.array([56.0])])

    # Extract section from parent domain of NEMODataTree:
    ds_bdy = nemo.extract_section(
        lon_section=lon_osnap,
        lat_section=lat_osnap,
        uv_vars=['uo', 'vo'],
        vars=['thetao_con', 'so_abs'],
        dom='.',
        )

    # Add volume transport normal to OSNAP section:
    ds_bdy['volume_transport'] = (
        ds_bdy['velocity'] * ds_bdy['e1b'] * ds_bdy['e3b']
        ).rename('volume_transport')

    # Add CF attributes:
    ds_bdy['volume_transport'].attrs['long_name'] = 'volume transport normal to OSNAP section'
    ds_bdy['volume_transport'].attrs['units'] = 'm3 s-1'

    return ds_bdy