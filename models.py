from pydantic import BaseModel
from typing import List, Dict
from datetime import datetime


class LocationConfig(BaseModel):
    name: str
    xmin: float
    xmax: float
    ymin: float
    ymax: float


class ServerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8000
    refresh_interval_minutes: int = 60


class Config(BaseModel):
    server: ServerConfig
    locations: Dict[str, LocationConfig]


class HourlyWeatherData(BaseModel):
    hour: int  # Hours from reference time
    datetime: datetime
    temperature_min: float
    temperature_max: float
    temperature_mean: float
    precipitation: float  # mm/h


class WeatherForecast(BaseModel):
    location_slug: str
    location_name: str
    reference_time: datetime
    last_updated: datetime
    forecast_hours: int
    bounding_box: Dict[str, float]
    hourly_data: List[HourlyWeatherData]
    summary: Dict[str, float]


class ErrorResponse(BaseModel):
    error: str
    message: str
