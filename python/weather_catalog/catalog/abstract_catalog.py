from abc import ABC, abstractmethod

from weather_catalog.basemodel import BaseModel
from weather_catalog.data import DataCube
from weather_catalog.query import Query

from .abstract_data_downloader import AbstractDataDownloader
from .abstract_data_uploader import AbstractDataUploader


class AbstractCatalog(ABC, BaseModel):

    catalog_id: str

    downloader: AbstractDataDownloader
    uploader: AbstractDataUploader

    @abstractmethod
    def can_source(self, query: Query) -> bool:
        pass

    def get_data(self, query: Query) -> DataCube:
        return self.downloader.download_data(query)

    def upload_data(self, data: DataCube, query: Query) -> bool:
        return self.uploader.upload_data(data, query)
