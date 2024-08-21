import os

import zarr

from weather_catalog.catalog.abstract_data_downloader import AbstractDataDownloader
from weather_catalog.data import DataCube
from weather_catalog.data.zarray_data_cube import ZarrayDataCube
from weather_catalog.enums import WeatherVariable
from weather_catalog.query import Query


class LocalDataDownloader(AbstractDataDownloader):

    base_path: str

    def download_data(self, query: Query) -> DataCube:
        path = self._convert_query_to_relative_path(query)
        data_group = zarr.open(path, mode="r")
        return ZarrayDataCube(
            dataset=data_group,
            variable_rename_map={
                WeatherVariable.TEMPERATURE: "t2m",
                WeatherVariable.WIND_U: "u10m",
                WeatherVariable.WIND_V: "v10m",
            },
        )

    def _convert_query_to_relative_path(self, query: Query) -> str:
        return os.path.join(
            self.base_path, query.weather_model_group, f"{query.weather_model_id}.zarr"
        )
