from enum import Enum


class WeatherVariable(Enum):
    TEMPERATURE = "temperature"
    PRESSURE = "pressure"
    HUMIDITY = "humidity"
    WIND_U = "wind_u"
    WIND_V = "wind_v"
