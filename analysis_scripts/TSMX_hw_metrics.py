import sys
sys.path.insert(1, '../heatwave_diagnostics_package/src')
import hdp
import numpy as np
import xarray
from os import listdir


if __name__ == '__main__':
    threshold_dataset = xarray.open_dataset("../../DATA/ANALYSIS_OUTPUT/HEAT_OUTPUT/THRESHOLDS/TSMX_LENS2_PI_threshold.nc")
    RAMIP_DIR = "/projects/dgs/persad_research/RAMIP/"

    paths = [name for name in listdir(RAMIP_DIR) if "tasmax_day_CESM2" in name]

    for index, name in enumerate(paths):
        print(round(100*index/len(paths)), end="%, ")
        variable = name.split("_")[0]
        model = name.split("_")[2]
        exp = name.split("_")[3]
        em = name.split("_")[4]
        dates = name.split("_")[-1].split(".")[0]
        OUT_PATH = f"../../DATA/ANALYSIS_OUTPUT/HEAT_OUTPUT/METRICS/{variable}_{model}_{exp}_{em}_{dates}_metrics.nc"

        hw_ds = hdp.compute_heatwave_metrics(xarray.open_dataset(RAMIP_DIR + name)["tasmax"], threshold_dataset)
        hw_ds.to_netcdf(OUT_PATH)