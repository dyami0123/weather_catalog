import io
from abc import abstractmethod

from weather_catalog.catalog.abstract_data_uploader import AbstractDataUploader
from weather_catalog.data import DataCube
from weather_catalog.query import Query

from .session.s3_session import S3Session


class S3Uploader(AbstractDataUploader):

    bucket_base_path: str

    def upload_data(self, data: DataCube, query: Query) -> bool:
        path = self._convert_query_to_relative_path(query)
        with S3Session.s3_fs.open(path, "wb") as f:
            return self.write_data(f, data)

    @abstractmethod
    def _convert_query_to_relative_path(self, query: Query) -> str:
        pass

    @abstractmethod
    def write_data(self, file: io.BytesIO, data: DataCube) -> bool:
        pass
