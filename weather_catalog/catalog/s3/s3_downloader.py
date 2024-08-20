import io
from abc import abstractmethod

from weather_catalog.catalog.abstract_data_downloader import AbstractDataDownloader
from weather_catalog.data import DataCube
from weather_catalog.query import Query

from .session.s3_session import S3Session


class S3Downloader(AbstractDataDownloader):

    bucket_base_path: str

    def download_data(self, query: Query) -> DataCube:
        path = self._convert_query_to_relative_path(query)
        with S3Session.s3_fs.open(path, "rb") as f:
            return self.read_in_data(f)

    @abstractmethod
    def _convert_query_to_relative_path(self, query: Query) -> str:
        pass

    @abstractmethod
    def read_in_data(self, file: io.BytesIO) -> DataCube:
        pass
