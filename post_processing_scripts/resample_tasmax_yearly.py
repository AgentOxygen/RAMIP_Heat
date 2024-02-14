import xarray
import matplotlib.pyplot as plt
import numpy as np
from os import listdir
import dask
import cftime


def concat_func(paths):
    datasets = []
    start = cftime.DatetimeNoLeap(2015, 12, 31, 0, 0, 0, 0, has_year_zero=True)
    end = cftime.DatetimeNoLeap(2079, 12, 31, 0, 0, 0, 0, has_year_zero=True)
    for path in paths:
        datasets.append(xarray.open_dataset(path)["tasmax"].sel(time=slice(start, end)).resample(time="Y").mean(keep_attrs=True))
    return xarray.concat(datasets, dim="member")


if __name__ == '__main__':
    print("Init")

    start = cftime.DatetimeNoLeap(2015, 12, 31, 0, 0, 0, 0, has_year_zero=True)
    end = cftime.DatetimeNoLeap(2079, 12, 31, 0, 0, 0, 0, has_year_zero=True)

    RAMIP_DIR = "/projects/dgs/persad_research/RAMIP/"

    paths = [RAMIP_DIR + name for name in listdir(RAMIP_DIR) if "tasmax_day_CESM2_ssp370-LE" in name]
    paths.sort()
    tasmax_LE = concat_func(paths)

    print("1")

    paths = [RAMIP_DIR + name for name in listdir(RAMIP_DIR) if "tasmax_day_CESM2_ssp370-126" in name]
    paths.sort()
    tasmax_GLOBAL = concat_func(paths)

    print("2")

    paths = [RAMIP_DIR + name for name in listdir(RAMIP_DIR) if "tasmax_day_CESM2_ssp370-NAE" in name]
    paths.sort()
    tasmax_NAE = concat_func(paths)

    print("3")

    paths = [RAMIP_DIR + name for name in listdir(RAMIP_DIR) if "tasmax_day_CESM2_ssp370-EAS" in name]
    paths.sort()
    tasmax_EAS = concat_func(paths)

    print("4")

    paths = [RAMIP_DIR + name for name in listdir(RAMIP_DIR) if "tasmax_day_CESM2_ssp370-AFR" in name]
    paths.sort()
    tasmax_AFR = concat_func(paths)

    print("5")

    paths = [RAMIP_DIR + name for name in listdir(RAMIP_DIR) if "tasmax_day_CESM2_ssp370-SAS" in name]
    paths.sort()
    tasmax_SAS = concat_func(paths)

    print("Chunked...")

    tasmax_datasets = xarray.Dataset(
        data_vars={
            "TASMAX_LE": (["member", "year", "lat", "lon"], tasmax_LE.resample(time="Y").mean().values),
            "TASMAX_GLOBAL": (["member", "year", "lat", "lon"], tasmax_GLOBAL.resample(time="Y").mean().values),
            "TASMAX_NAE": (["member", "year", "lat", "lon"], tasmax_NAE.resample(time="Y").mean().values),
            "TASMAX_EAS": (["member", "year", "lat", "lon"], tasmax_EAS.resample(time="Y").mean().values),
            "TASMAX_AFR": (["member", "year", "lat", "lon"], tasmax_AFR.resample(time="Y").mean().values),
            "TASMAX_SAS": (["member", "year", "lat", "lon"], tasmax_SAS.resample(time="Y").mean().values),
        },
        coords=dict(
            lon=tasmax_LE.lon.values,
            lat=tasmax_LE.lat.values,
            year=np.arange(2015, 2080),
            member=[path.split("/")[-1].split("_")[4] for path in paths]
        ),
        attrs=tasmax_LE.attrs,
    )

    tasmax_datasets.to_netcdf("/projects/dgs/persad_research/cummins_ramip/DATA/ANALYSIS_OUTPUT/post_processing/CESM2_tasmax_resampled_yearly.nc")