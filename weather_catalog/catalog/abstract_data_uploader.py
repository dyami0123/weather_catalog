from abc import ABC, abstractmethod

from weather_catalog.basemodel import BaseModel
from weather_catalog.data import DataCube
from weather_catalog.query import Query


class AbstractDataUploader(ABC, BaseModel):

    @abstractmethod
    def upload_data(self, data: DataCube, query: Query) -> bool:
        pass
