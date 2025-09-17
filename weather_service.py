import logging
import os
import shutil
import tempfile
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
        self.temp_cache_dir = tempfile.mkdtemp(prefix="weather_cache_")
        config.set("cache-directory", self.temp_cache_dir)
        config.set("cache-policy", "user")

    def clear_earthkit_cache(self):
        """Clear the earthkit cache directory."""
        try:
            if os.path.exists(self.temp_cache_dir):
                shutil.rmtree(self.temp_cache_dir)
                # Recreate the directory
                os.makedirs(self.temp_cache_dir, exist_ok=True)
                logger.info("Cleared earthkit cache directory")
        except Exception as e:
            logger.warning(f"Could not clear earthkit cache: {e}")

    async def get_forecast_for_location(
        self,
        slug: str,
        location: LocationConfig,
        hours_ahead: int = 48,
        reference_time: Optional[datetime] = None,
    ) -> WeatherForecast:
        """
        Get weather forecast for a specific location.
        """
        # Using a separate function to ensure proper cleanup
        try:
            logger.info(f"Fetching weather data for {slug} ({location.name})")

            # Set default reference time if not provided
            if reference_time is None:
                reference_time = datetime.now(timezone.utc).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )

            # Create bounding box dict
            bbox = {
                "xmin": location.xmin,
                "xmax": location.xmax,
                "ymin": location.ymin,
                "ymax": location.ymax,
            }

            # Process one batch at a time to minimize memory usage
            forecast = await self._process_location_data(
                slug, location.name, bbox, hours_ahead, reference_time
            )

            logger.info(f"Successfully fetched weather data for {slug}")
            return forecast

        except Exception as e:
            logger.error(f"Error fetching weather data for {slug}: {e}")
            raise

    async def _process_location_data(
        self,
        slug: str,
        location_name: str,
        bbox: Dict[str, float],
        hours_ahead: int,
        reference_time: datetime,
    ) -> WeatherForecast:
        """
        Process weather data for a location, ensuring memory is released.
        """
        temp_data = None
        precip_data = None
        hourly_data = []

        try:
            # Get weather data
            weather_data = await self._fetch_weather_data(
                bbox, hours_ahead, reference_time
            )

            temp_data = weather_data["temperature_data"]
            precip_data = weather_data["precipitation_data"]

            # Process into hourly format - extract data and release immediately
            hourly_data = self._process_hourly_data(
                temp_data,
                precip_data,
                reference_time,
            )

            # Calculate summary statistics
            summary = self._calculate_summary(hourly_data)

            # Create forecast object with the extracted data
            forecast = WeatherForecast(
                location_slug=slug,
                location_name=location_name,
                reference_time=reference_time,
                last_updated=datetime.now(timezone.utc),
                forecast_hours=hours_ahead,
                bounding_box=bbox,
                hourly_data=hourly_data,
                summary=summary,
            )

            return forecast

        finally:
            # Aggressively close and clean up all data
            if temp_data is not None and hasattr(temp_data, "close"):
                temp_data.close()
                temp_data = None

            if precip_data is not None and hasattr(precip_data, "close"):
                precip_data.close()
                precip_data = None

            # Force garbage collection to clean up any remaining data
            import gc

            gc.collect()
            # Clear earthkit data after processing
            self.clear_earthkit_cache()

    async def _fetch_weather_data(
        self, bbox: Dict[str, float], hours_ahead: int, reference_time: datetime
    ) -> Dict:
        """
        Fetch raw weather data from the API, with aggressive memory management.
        """
        import gc  # Import garbage collector

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

        temp_data_list = []
        precip_data_list = []
        temp_data = None
        precip_data = None
        precip_hourly = None
        temp_regridded = None
        precip_regridded = None

        try:
            # Retrieve temperature data in batches and process immediately
            logger.info(f"Fetching {len(temp_requests)} temperature requests")
            for req in temp_requests:
                da = ogd_api.get_from_ogd(req)
                temp_data_list.append(da)

            # Retrieve precipitation data in batches
            logger.info(f"Fetching {len(precip_requests)} precipitation requests")
            for req in precip_requests:
                da = ogd_api.get_from_ogd(req)
                precip_data_list.append(da)

            # Merge data arrays
            logger.info("Merging data arrays")
            temp_data = xr.concat(temp_data_list, dim="lead_time")
            precip_data = xr.concat(precip_data_list, dim="lead_time")

            # Clear lists to free memory
            for da in temp_data_list:
                if hasattr(da, "close"):
                    da.close()
            temp_data_list = []

            for da in precip_data_list:
                if hasattr(da, "close"):
                    da.close()
            precip_data_list = []

            # Run garbage collection after clearing lists
            gc.collect()

            # Convert precipitation to hourly values (deaccumulate)
            logger.info("Processing precipitation data")
            precip_hourly = time_ops.delta(precip_data, np.timedelta64(1, "h"))

            # Close precipitation data as soon as we're done with it
            if hasattr(precip_data, "close"):
                precip_data.close()
                precip_data = None
                gc.collect()  # Run garbage collection

            # Define target grid for the bounding box
            xmin, xmax = bbox["xmin"], bbox["xmax"]
            ymin, ymax = bbox["ymin"], bbox["ymax"]

            # Calculate grid resolution
            nx = max(int((xmax - xmin) * 111.32 / 2), 10)
            ny = max(int((ymax - ymin) * 111.32 / 2), 10)

            # Create target grid
            logger.info("Creating regridding destination")
            destination = regrid.RegularGrid(
                CRS.from_string("epsg:4326"), nx, ny, xmin, xmax, ymin, ymax
            )

            # Regrid data to regular lat/lon grid one at a time
            logger.info("Regridding temperature data")
            temp_regridded = regrid.iconremap(temp_data, destination)

            # Close temperature data after regridding
            if hasattr(temp_data, "close"):
                temp_data.close()
                temp_data = None
                gc.collect()  # Run garbage collection

            logger.info("Regridding precipitation data")
            precip_regridded = regrid.iconremap(precip_hourly, destination)

            # Close precipitation hourly data after regridding
            if hasattr(precip_hourly, "close"):
                precip_hourly.close()
                precip_hourly = None
                gc.collect()  # Run garbage collection

            return {
                "temperature_data": temp_regridded,
                "precipitation_data": precip_regridded,
            }
        except Exception as e:
            logger.error(f"Error in _fetch_weather_data: {e}")
            raise
        finally:
            # Explicitly close all data arrays to prevent memory leaks
            for da in temp_data_list:
                if hasattr(da, "close"):
                    da.close()

            for da in precip_data_list:
                if hasattr(da, "close"):
                    da.close()

            # Close all data objects
            for data_obj in [temp_data, precip_data, precip_hourly]:
                if data_obj is not None and hasattr(data_obj, "close"):
                    data_obj.close()

            # Run a final garbage collection
            gc.collect()

    def _process_hourly_data(
        self,
        temp_data: xr.DataArray,
        precip_data: xr.DataArray,
        reference_time: datetime,
    ) -> List[HourlyWeatherData]:
        """
        Process raw data into hourly format with careful memory management.
        """
        import gc  # Import garbage collector

        hourly_data = []

        # Get lead times once
        lead_times = [
            int(lt.astype("timedelta64[h]").astype(int))
            for lt in temp_data.lead_time.values
        ]

        # Skip first hour for precipitation (no delta available)
        for i in range(1, len(lead_times)):
            lead_time_hours = lead_times[i]
            hour_datetime = reference_time + timedelta(hours=lead_time_hours)

            # Extract temperature data (convert from Kelvin to Celsius) in isolated blocks
            with temp_data.isel(lead_time=i) as temp_slice:
                temp_slice_celsius = temp_slice - 273.15
                temp_min = float(temp_slice_celsius.min().values)
                temp_max = float(temp_slice_celsius.max().values)
                temp_mean = float(temp_slice_celsius.mean().values)
                # Let temp_slice go out of scope

            # Extract precipitation data (mm/h) in isolated blocks
            with precip_data.isel(lead_time=i) as precip_slice:
                precip_max = float(precip_slice.max().values)
                # Let precip_slice go out of scope

            # Create hourly data object with extracted scalar values
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

            # Run garbage collection every 5 hours to keep memory usage low
            if i % 5 == 0:
                gc.collect()

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
