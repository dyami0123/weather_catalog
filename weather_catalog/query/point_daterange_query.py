from datetime import datetime

from weather_catalog.enums import Frequency, WeatherVariable

from .location.point_location import PointLocation
from .query import Query


class PointDateRangeQuery(Query):
    start_date: datetime
    end_date: datetime
    location: PointLocation
    weather_model_group: str
    weather_model_id: str
    variable: WeatherVariable
    frequency: Frequency
