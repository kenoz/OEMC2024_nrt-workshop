import datetime

from nrt import data


cube = data.germany_zarr()
cube_sub = cube[['B08', 'B11', 'B12', 'SCL']].sel(time=slice(datetime.datetime(2018,1,1),
                                                             datetime.datetime(2021, 12, 31)))
# Loading data in memory takes a long time as it needs to be downloaded from server via http. Maybe wise to get a copy saved as netcdf before workshop
# and use that instead of zarr loading
cube_sub.load()
encoding = {
    'B08': {'dtype': 'int16', 'scale_factor': 0.001, 'zlib': True, 'complevel': 4, '_FillValue': -9999},
    'B11': {'dtype': 'int16', 'scale_factor': 0.001, 'zlib': True, 'complevel': 4, '_FillValue': -9999},
    'B12': {'dtype': 'int16', 'scale_factor': 0.001, 'zlib': True, 'complevel': 4, '_FillValue': -9999},
    'SCL': {'dtype': 'uint8', 'zlib': True, 'complevel': 4}
}
cube_sub.to_netcdf('../data/germany.nc', encoding=encoding)