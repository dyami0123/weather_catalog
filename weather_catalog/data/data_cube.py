from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

import pandas as pd

from weather_catalog.basemodel import BaseModel
from weather_catalog.enums import WeatherVariable


class DataCube(BaseModel, ABC):

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
        pass
