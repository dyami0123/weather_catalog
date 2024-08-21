import io
import os

from weather_catalog.catalog.abstract_data_uploader import AbstractDataUploader
from weather_catalog.data import DataCube
from weather_catalog.query import Query


class LocalDataUploader(AbstractDataUploader):

    base_path: str

    def upload_data(self, data: DataCube, query: Query) -> bool:
        path = self._convert_query_to_relative_path(query)
        with open(path, "wb") as f:
            return self.write_data(f, data)  # type: ignore

    def _convert_query_to_relative_path(self, query: Query) -> str:
        return os.path.join(
            self.base_path, query.weather_model_group, f"{query.weather_model_id}.zarr"
        )

    def write_data(self, file: io.BytesIO, data: DataCube) -> bool:
        raise NotImplementedError
