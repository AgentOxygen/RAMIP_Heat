import sys
sys.path.insert(1, '../heatwave_diagnostics_package/src')
import hdp
import numpy as np
import xarray
from os import listdir


if __name__ == '__main__':
    PI_CONTROL_DATA_DIR = "../../DATA/LENS2_1850_CONTROL/TSMX/DAILY/"
    THRESHOLD_DATA_DIR = "/projects/dgs/persad_research/cummins_ramip/DATA/ANALYSIS_OUTPUT/HEAT_OUTPUT/THRESHOLDS/"

    datasets = []
    full_paths = []
    for index in range(1, 11):
        ens_mem = str(index).zfill(3)

        paths = [PI_CONTROL_DATA_DIR + name for name in listdir(PI_CONTROL_DATA_DIR) if f".{ens_mem}." in name]
        for path in paths:
            try:
                datasets.append(xarray.open_dataset(path))
                full_paths.append(path)
            except OSError:
                print(path)
        print(index, end=", ")

    baseline_dataset = xarray.concat(datasets, dim="time")
    threshold_dataset = hdp.compute_threshold(baseline_dataset["TSMX"], np.arange(0.9, 1.0, 0.01), temp_path=str(full_paths))