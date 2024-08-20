from typing import Type

from weather_catalog.catalog.abstract_catalog import AbstractCatalog
from weather_catalog.catalog.s3.s3_downloader import S3Downloader
from weather_catalog.catalog.s3.s3_uploader import S3Uploader
from weather_catalog.data import DataCube
from weather_catalog.query import Query

from .session.s3_session import S3Session


class S3Catalog(AbstractCatalog):

    downloader: S3Downloader
    uploader: S3Uploader

    _downloader_class: Type[S3Downloader]
    _uploader_class: Type[S3Uploader]

    def __init__(self):
        self._downloader = self._downloader_class()
        self._uploader = self._uploader_class()

    def can_source(self, query: Query) -> bool:
        bucket_path = self.downloader._convert_query_to_relative_path(query)
        return S3Session.s3_fs.exists(bucket_path)

    def get_data(self, query: Query) -> DataCube:
        return self.downloader.download_data(query)

    def upload_data(self, data: DataCube, query: Query) -> bool:
        return self.uploader.upload_data(data, query)
