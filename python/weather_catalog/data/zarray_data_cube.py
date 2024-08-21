from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
from zarr.hierarchy import Group

from weather_catalog.enums import Coordinate, WeatherVariable

from .data_cube import DataCube


class ZarrayDataCube(DataCube):
    dataset: Group

    variable_rename_map: Optional[dict[WeatherVariable, str]] = None

    _coordinate_index_map: dict[Coordinate, int] = {
        Coordinate.LATITUDE: 0,
        Coordinate.LONGITUDE: 1,
        Coordinate.TIME: 2,
    }

    _coordinate_rename_map: dict[Coordinate, str] = {
        Coordinate.LATITUDE: "latitude",
        Coordinate.LONGITUDE: "longitude",
        Coordinate.TIME: "time",
    }

    def get_data(
        self,
        latitude: float,
        longitude: float,
        start_date: datetime,  # TODO: incorporate end_date & Start date
        end_date: datetime,
        variables: list[WeatherVariable],
    ) -> pd.DataFrame:
        """Pull data for a single geospatial location and time range

        Args:
            latitude (float): The latitude of the location
            longitude (float): The longitude of the location
            start_date (datetime): The start date of the time range
            end_date (datetime): The end date of the time range
            variables (list[WeatherVariable]): The variables to pull data for

        Returns:
            pd.DataFrame: A pandas dataframe of the data
        """

        if self.variable_rename_map is not None:
            variable_strings = [self.variable_rename_map[v] for v in variables]
        else:
            variable_strings = [v.value for v in variables]

        lat_index = self._closest_coodinate_index(Coordinate.LATITUDE, latitude)
        lon_index = self._closest_coodinate_index(Coordinate.LONGITUDE, longitude)

        var_output_dict = {
            variable.value: self.dataset[variable_string][
                :, lat_index, lon_index
            ]  # TODO: how to use the coordinate index map here?
            for variable, variable_string in zip(variables, variable_strings)
        }

        return pd.DataFrame(var_output_dict)

    def _closest_coodinate_index(self, coordinate: Coordinate, value: float) -> int:
        coordinate_values = np.array(
            self.dataset[self._coordinate_rename_map[coordinate]]
        )
        return int(np.argmin(np.abs(coordinate_values - value)))
