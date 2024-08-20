import datetime

from weather_catalog.basemodel import BaseModel
from weather_catalog.enums import Frequency, Resolution, WeatherVariable

from .location.abstract_location import AbstractLocation


class Query(BaseModel):
    start_date: datetime.datetime
    end_date: datetime.datetime
    frequency: Frequency
    location: AbstractLocation
    weather_model_group: str
    weather_model_id: str
    resolution: Resolution
    variable: WeatherVariable
