import asyncio
import gc  # Import garbage collector
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
        """Update weather data for all configured locations, one at a time to minimize memory usage."""

        logger.info("Updating weather data for all locations")

        reference_time = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        # Process locations one at a time to limit memory usage
        locations = list(self.config.locations.items())
        logger.info(f"Processing {len(locations)} locations sequentially to save memory")
        
        for slug, location in locations:
            try:
                logger.info(f"Processing location: {slug}")
                await self.update_location(slug, location, reference_time)
                logger.info(f"Successfully updated {slug}")
            except Exception as e:
                logger.error(f"Failed to update {slug}: {e}")
            
            # Force garbage collection after each location
            collected = gc.collect()
            logger.info(f"Garbage collection after {slug}: {collected} objects collected")
        
        # Run a final round of garbage collection
        collected = gc.collect()
        logger.info(f"Final garbage collection completed: {collected} objects collected")

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
        finally:
            collected = gc.collect()
            logger.info(f"Garbage collection completed: {collected} objects collected")

    def stop(self):
        """Stop the background update task."""
        self.running = False
        logger.info("Stopping background weather updates")
