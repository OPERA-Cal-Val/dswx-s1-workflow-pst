from pathlib import Path

import numpy as np
from pysheds.grid import Grid


def compute_hand(dem_path: Path, acc_thresh: float = 100) -> np.ndarray:
    """
    Sources:

    + https://github.com/mdbartos/pysheds/blob/master/examples/hand.ipynb
    + https://github.com/mdbartos/pysheds#example-usage
    + https://github.com/ASFHyP3/asf-tools/blob/561b6aaf0399f61310ac8531d789f83bf769c347/src/asf_tools/hand/calculate.py#L58

    acc_thresh set according to last reference. The original example in pysheds sets to 200.
    """
    dem_path_str = str(dem_path)
    grid = Grid.from_raster(dem_path_str)
    dem = grid.read_raster(dem_path_str, data_name='dem')

    pit_filled_dem = grid.fill_pits(dem)
    flooded_dem = grid.fill_depressions(pit_filled_dem)

    inflated_dem = grid.resolve_flats(flooded_dem)

    # Integers assigned to cardinal directions and intermediates - see flowdir docstring
    dirmap = (64, 128, 1, 2, 4, 8, 16, 32)
    fdir = grid.flowdir(inflated_dem, dirmap=dirmap)

    acc = grid.accumulation(fdir, dirmap=dirmap)
    hand = grid.compute_hand(fdir, dem, acc > acc_thresh, inplace=False)
    return hand.__array__()
