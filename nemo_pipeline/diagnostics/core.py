"""
core.py

Description: Core diagnostic functions for NEMO Pipeline.

Created By: Ollie Tooth (oliver.tooth@noc.ac.uk)
Date Created: 03/11/2025
"""

# -- Import dependencies -- #
import gsw
import numpy as np
import xarray as xr
from nemo_cookbook import NEMODataTree
from nemo_cookbook.stats import compute_binned_statistic


# -- Core Diagnostics -- #
def extract_osnap_section(
    nemo: NEMODataTree,
    include_eiv: bool = False,
    ) -> xr.Dataset:
    """
    Extract Overturning in the Subpolar North Atlantic Program
    (OSNAP) hydrographic section from NEMODataTree.

    Parameters:
    -----------
    nemo : NEMODataTree
        A hierarchical data tree of NEMO model outputs.
    include_eiv : bool, optional
        Whether to calculate the total velocity from resolved
        & eddy-induced velocity (eiv) variables. Default is False.

    Returns:
    --------
    xr.Dataset
        Velocity, volume transport, conservative temperature and
        absolute salinity data extracted along the OSNAP section.
    """
    # Validate inputs:
    if not isinstance(nemo, NEMODataTree):
        raise TypeError("nemo must be a NEMODataTree object.")

    # Open OSNAP coords from gridded observations dataset:
    ds_osnap = xr.open_zarr("https://noc-msm-o.s3-ext.jc.rl.ac.uk/ocean-obs/OSNAP/OSNAP_Gridded_TSV_201408_202006_2023")
    # Define OSNAP section coordinates (adding final land point - UK):
    lon_osnap = np.concatenate([ds_osnap['LONGITUDE'].values, np.array([-4.0])])
    lat_osnap = np.concatenate([ds_osnap['LATITUDE'].values, np.array([56.0])])

    if include_eiv:
        # Calculate total velocity = resolved + eddy-induced velocity (eiv):
        if ('uo_eiv' not in nemo['/gridU'].data_vars) or ('vo_eiv' not in nemo['/gridV'].data_vars):
            raise ValueError("variables 'uo_eiv' and 'vo_eiv' not found in NEMODataTree.")
        nemo['/gridU']['u_total'] = nemo['/gridU']['uo'] + nemo['/gridU']['uo_eiv']
        nemo['/gridV']['v_total'] = nemo['/gridV']['vo'] + nemo['/gridV']['vo_eiv']
    else:
        # Use resolved velocity only:
        nemo['/gridU']['u_total'] = nemo['/gridU']['uo']
        nemo['/gridV']['v_total'] = nemo['/gridV']['vo']

    # Extract section from parent domain of NEMODataTree:
    ds_bdy = nemo.extract_section(
        lon_section=lon_osnap,
        lat_section=lat_osnap,
        uv_vars=['u_total', 'v_total'],
        vars=['thetao_con', 'so_abs'],
        dom='.',
        )

    # Add potential density reference to sea surface:
    ds_bdy['sigma0'] = gsw.density.sigma0(CT=ds_bdy['thetao_con'], SA=ds_bdy['so_abs'])
    ds_bdy['sigma0'].name = 'sigma0'

    # Add volume transport normal to OSNAP section:
    ds_bdy['volume_transport'] = (
        ds_bdy['velocity'] * ds_bdy['e1b'] * ds_bdy['e3b']
        ).rename('volume_transport')

    # Define potential density bins:
    sigma0_bins = np.arange(20, 29, 0.01)

    # Compute Total OSNAP diapycnal overturning stream function:
    ds_bdy['moc_total'] = compute_binned_statistic(vars=[ds_bdy['sigma0']],
                                                   values=ds_bdy['volume_transport'],
                                                   keep_dims=['time_counter'],
                                                   bins=[sigma0_bins],
                                                   statistic='nansum',
                                                   mask=None
                                                   ).cumsum(dim='sigma0_bins')

    # Determine index to split OSNAP West & OSNAP East sections:
    station_OWest_OEast = ds_bdy['bdy'].where(ds_bdy['glamb'] <= -44).max()

    # OSNAP East diapycnal overturning stream function:
    mask_OEast = ds_bdy['bdy'] >= station_OWest_OEast
    ds_bdy['moc_east'] = compute_binned_statistic(vars=[ds_bdy['sigma0']],
                                                  values=ds_bdy['volume_transport'],
                                                  keep_dims=['time_counter'],
                                                  bins=[sigma0_bins],
                                                  statistic='nansum',
                                                  mask=mask_OEast
                                                  ).cumsum(dim='sigma0_bins')

    # OSNAP West diapycnal overturning stream function:
    mask_OWest = ds_bdy['bdy'] < station_OWest_OEast
    ds_bdy['moc_west'] = compute_binned_statistic(vars=[ds_bdy['sigma0']],
                                                  values=ds_bdy['volume_transport'],
                                                  keep_dims=['time_counter'],
                                                  bins=[sigma0_bins],
                                                  statistic='nansum',
                                                  mask=mask_OWest
                                                  ).cumsum(dim='sigma0_bins')

    # Add CF attributes:
    ds_bdy['velocity'].attrs['long_name'] = 'velocity normal to OSNAP section'
    ds_bdy['velocity'].attrs['units'] = 'm s-1'
    ds_bdy['thetao_con'].attrs['long_name'] = 'conservative temperature'
    ds_bdy['thetao_con'].attrs['units'] = 'degC'
    ds_bdy['so_abs'].attrs['long_name'] = 'absolute salinity'
    ds_bdy['so_abs'].attrs['units'] = 'g kg-1'
    ds_bdy['sigma0'].attrs['long_name'] = 'potential density anomaly referenced to sea surface'
    ds_bdy['sigma0'].attrs['units'] = 'kg m-3'
    ds_bdy['volume_transport'].attrs['long_name'] = 'volume transport normal to OSNAP section'
    ds_bdy['volume_transport'].attrs['units'] = 'm3 s-1'
    ds_bdy['moc_total'].attrs['long_name'] = 'total OSNAP diapycnal overturning stream function'
    ds_bdy['moc_total'].attrs['units'] = 'm3 s-1'
    ds_bdy['moc_east'].attrs['long_name'] = 'OSNAP East diapycnal overturning stream function'
    ds_bdy['moc_east'].attrs['units'] = 'm3 s-1'
    ds_bdy['moc_west'].attrs['long_name'] = 'OSNAP West diapycnal overturning stream function'
    ds_bdy['moc_west'].attrs['units'] = 'm3 s-1'

    return ds_bdy