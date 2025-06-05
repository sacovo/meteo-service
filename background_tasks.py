import asyncio
import logging
from datetime import datetime, timezone

from models import Config
from weather_service import WeatherService

logger = logging.getLogger(__name__)


class WeatherUpdateTask:
    def __init__(self, weather_service: WeatherService, config: Config):
        self.weather_service = weather_service
        self.config = config
        self.running = False

    async def start_background_updates(self):
        """Start the background task for updating weather data."""
        self.running = True
        logger.info("Starting background weather updates")

        # Initial fetch on startup
        await self.update_all_locations()

        # Schedule periodic updates
        while self.running:
            await asyncio.sleep(self.config.server.refresh_interval_minutes * 60)
            if self.running:
                await self.update_all_locations()

    async def update_all_locations(self):
        """Update weather data for all configured locations."""
        logger.info("Updating weather data for all locations")

        reference_time = datetime.now(timezone.utc)

        tasks = []
        for slug, location in self.config.locations.items():
            task = self.update_location(slug, location, reference_time)
            tasks.append(task)

        # Update all locations concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Log results
        for i, result in enumerate(results):
            slug = list(self.config.locations.keys())[i]
            if isinstance(result, Exception):
                logger.error(f"Failed to update {slug}: {result}")
            else:
                logger.info(f"Successfully updated {slug}")

    async def update_location(self, slug: str, location, reference_time: datetime):
        """Update weather data for a single location."""
        try:
            forecast = await self.weather_service.get_forecast_for_location(
                slug, location, reference_time=reference_time
            )
            self.weather_service.update_cache(slug, forecast)
        except Exception as e:
            logger.error(f"Error updating {slug}: {e}")
            raise

    def stop(self):
        """Stop the background update task."""
        self.running = False
        logger.info("Stopping background weather updates")
