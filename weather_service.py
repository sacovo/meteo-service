import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import numpy as np
import xarray as xr
from earthkit.data import config
from meteodatalab import ogd_api
from meteodatalab.operators import regrid
from meteodatalab.operators import time_operators as time_ops
from rasterio.crs import CRS

from models import HourlyWeatherData, LocationConfig, WeatherForecast

# Set temporary caching
config.set("cache-policy", "user")

logger = logging.getLogger(__name__)


class WeatherService:
    def __init__(self):
        self.cached_forecasts: Dict[str, WeatherForecast] = {}

    async def get_forecast_for_location(
        self,
        slug: str,
        location: LocationConfig,
        hours_ahead: int = 24,
        reference_time: Optional[datetime] = None,
    ) -> WeatherForecast:
        """
        Get weather forecast for a specific location.
        """
        try:
            logger.info(f"Fetching weather data for {slug} ({location.name})")

            # Set default reference time if not provided
            if reference_time is None:
                reference_time = datetime.now(timezone.utc)

            # Create bounding box dict
            bbox = {
                "xmin": location.xmin,
                "xmax": location.xmax,
                "ymin": location.ymin,
                "ymax": location.ymax,
            }

            # Get weather data
            weather_data = await self._fetch_weather_data(
                bbox, hours_ahead, reference_time
            )

            # Process into hourly format
            hourly_data = self._process_hourly_data(
                weather_data["temperature_data"],
                weather_data["precipitation_data"],
                reference_time,
            )

            # Calculate summary statistics
            summary = self._calculate_summary(hourly_data)

            # Create forecast object
            forecast = WeatherForecast(
                location_slug=slug,
                location_name=location.name,
                reference_time=reference_time,
                last_updated=datetime.now(timezone.utc),
                forecast_hours=hours_ahead,
                bounding_box=bbox,
                hourly_data=hourly_data,
                summary=summary,
            )

            logger.info(f"Successfully fetched weather data for {slug}")
            return forecast

        except Exception as e:
            logger.error(f"Error fetching weather data for {slug}: {e}")
            raise

    async def _fetch_weather_data(
        self, bbox: Dict[str, float], hours_ahead: int, reference_time: datetime
    ) -> Dict:
        """
        Fetch raw weather data from the API.
        """
        # Create requests for both temperature and precipitation
        temp_requests = []
        precip_requests = []

        for i in range(hours_ahead + 1):
            # Temperature requests (2m temperature)
            temp_req = ogd_api.Request(
                collection="ogd-forecasting-icon-ch2",
                variable="T_2M",
                reference_datetime=reference_time,
                perturbed=False,
                horizon=f"P0DT{i}H",
            )
            temp_requests.append(temp_req)

            # Precipitation requests
            precip_req = ogd_api.Request(
                collection="ogd-forecasting-icon-ch2",
                variable="TOT_PREC",
                reference_datetime=reference_time,
                perturbed=False,
                horizon=f"P0DT{i}H",
            )
            precip_requests.append(precip_req)

        # Retrieve temperature data
        temp_data_list = []
        for req in temp_requests:
            da = ogd_api.get_from_ogd(req)
            temp_data_list.append(da)

        # Retrieve precipitation data
        precip_data_list = []
        for req in precip_requests:
            da = ogd_api.get_from_ogd(req)
            precip_data_list.append(da)

        # Merge data arrays
        temp_data = xr.concat(temp_data_list, dim="lead_time")
        precip_data = xr.concat(precip_data_list, dim="lead_time")

        # Convert precipitation to hourly values (deaccumulate)
        precip_hourly = time_ops.delta(precip_data, np.timedelta64(1, "h"))

        # Define target grid for the bounding box
        xmin, xmax = bbox["xmin"], bbox["xmax"]
        ymin, ymax = bbox["ymin"], bbox["ymax"]

        # Calculate grid resolution
        nx = max(int((xmax - xmin) * 111.32 / 2), 10)
        ny = max(int((ymax - ymin) * 111.32 / 2), 10)

        # Create target grid
        destination = regrid.RegularGrid(
            CRS.from_string("epsg:4326"), nx, ny, xmin, xmax, ymin, ymax
        )

        # Regrid data to regular lat/lon grid
        temp_regridded = regrid.iconremap(temp_data, destination)
        precip_regridded = regrid.iconremap(precip_hourly, destination)

        return {
            "temperature_data": temp_regridded,
            "precipitation_data": precip_regridded,
        }

    def _process_hourly_data(
        self,
        temp_data: xr.DataArray,
        precip_data: xr.DataArray,
        reference_time: datetime,
    ) -> List[HourlyWeatherData]:
        """
        Process raw data into hourly format.
        """
        hourly_data = []

        # Skip first hour for precipitation (no delta available)
        for i in range(1, len(temp_data.lead_time)):
            lead_time_hours = int(
                temp_data.lead_time.values[i].astype("timedelta64[h]").astype(int)
            )
            hour_datetime = reference_time + timedelta(hours=lead_time_hours)

            # Temperature data (convert from Kelvin to Celsius)
            temp_slice = temp_data.isel(lead_time=i) - 273.15
            temp_min = float(temp_slice.min().values)
            temp_max = float(temp_slice.max().values)
            temp_mean = float(temp_slice.mean().values)

            # Precipitation data (mm/h)
            precip_slice = precip_data.isel(lead_time=i)
            precip_max = float(precip_slice.max().values)

            hourly_data.append(
                HourlyWeatherData(
                    hour=lead_time_hours,
                    datetime=hour_datetime,
                    temperature_min=round(temp_min, 1),
                    temperature_max=round(temp_max, 1),
                    temperature_mean=round(temp_mean, 1),
                    precipitation=round(precip_max, 2),
                )
            )

        return hourly_data

    def _calculate_summary(
        self, hourly_data: List[HourlyWeatherData]
    ) -> Dict[str, float]:
        """
        Calculate summary statistics from hourly data.
        """
        if not hourly_data:
            return {}

        temps_min = [h.temperature_min for h in hourly_data]
        temps_max = [h.temperature_max for h in hourly_data]
        precip = [h.precipitation for h in hourly_data]

        return {
            "temperature_absolute_min": round(min(temps_min), 1),
            "temperature_absolute_max": round(max(temps_max), 1),
            "total_precipitation": round(sum(precip), 2),
            "max_hourly_precipitation": round(max(precip), 2),
            "min_hourly_precipitation": round(min(precip), 2),
            "average_temperature": round(
                sum(h.temperature_mean for h in hourly_data) / len(hourly_data), 1
            ),
        }

    def update_cache(self, slug: str, forecast: WeatherForecast):
        """Update the cache with new forecast data."""
        self.cached_forecasts[slug] = forecast
        logger.info(f"Updated cache for {slug}")

    def get_cached_forecast(self, slug: str) -> Optional[WeatherForecast]:
        """Get cached forecast data."""
        return self.cached_forecasts.get(slug)

    def clear_cache(self):
        """Clear all cached data."""
        self.cached_forecasts.clear()
        logger.info("Cleared weather cache")
