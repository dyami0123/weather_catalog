from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

import pandas as pd

from weather_catalog.basemodel import BaseModel
from weather_catalog.enums import WeatherVariable


class DataCube(BaseModel, ABC):
    """A data Cube is a generalized representation of a N dimensional array

    things like Zarr, NetCDF, and XArray are all Data Cubes.

    this wraps them in a shared API for use in the weather catalog library
    """

    _dataset: Any

    @abstractmethod
    def get_data(
        self,
        latitude: float,
        longitude: float,
        start_date: datetime,
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
        pass
