from abc import ABC, abstractmethod

from weather_catalog.basemodel import BaseModel
from weather_catalog.data import DataCube
from weather_catalog.query import Query


class AbstractDataDownloader(ABC, BaseModel):

    @abstractmethod
    def download_data(self, query: Query) -> DataCube:
        pass
