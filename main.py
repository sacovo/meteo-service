import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from background_tasks import WeatherUpdateTask
from config_loader import load_config
from models import WeatherForecast
from weather_service import WeatherService

# Configure logging with Docker-friendly format
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Console output for Docker logs
        (
            logging.FileHandler("/app/logs/weather_api.log")
            if os.path.exists("/app/logs")
            else logging.NullHandler()
        ),
    ],
)
logger = logging.getLogger(__name__)

# Global variables
weather_service = WeatherService()
config = load_config()
update_task = WeatherUpdateTask(weather_service, config)
background_task = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle startup and shutdown events."""
    global background_task

    # Startup
    logger.info("Starting weather forecast API")
    logger.info(f"Configured locations: {list(config.locations.keys())}")
    logger.info(f"Refresh interval: {config.server.refresh_interval_minutes} minutes")

    # Create cache directory if it doesn't exist
    cache_dir = ".cache"
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir, exist_ok=True)
        logger.info(f"Created cache directory: {cache_dir}")

    # Start background updates
    background_task = asyncio.create_task(update_task.start_background_updates())

    yield

    # Shutdown
    logger.info("Shutting down weather forecast API")
    update_task.stop()
    if background_task:
        background_task.cancel()
        try:
            await background_task
        except asyncio.CancelledError:
            logger.info("Background task cancelled successfully")


# Create FastAPI app
app = FastAPI(
    title="Weather Forecast API",
    description="MeteoSwiss weather forecast API for configured locations",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/forecast/{slug}", response_model=WeatherForecast)
async def get_forecast(slug: str):
    """
    Get weather forecast for a specific location by slug.

    Returns hourly temperature and precipitation data for the next 24 hours.
    """
    if slug not in config.locations:
        available_locations = list(config.locations.keys())
        raise HTTPException(
            status_code=404,
            detail=f"Location '{slug}' not found. Available locations: {available_locations}",
        )

    # Get cached forecast
    forecast = weather_service.get_cached_forecast(slug)

    if forecast is None:
        raise HTTPException(
            status_code=503,
            detail=f"Weather data for '{slug}' is not available yet. Please try again in a few moments.",
        )

    return forecast


@app.get("/locations")
async def get_locations():
    """Get list of all configured locations."""
    return {
        "locations": {
            slug: {
                "name": location.name,
                "bounding_box": {
                    "xmin": location.xmin,
                    "xmax": location.xmax,
                    "ymin": location.ymin,
                    "ymax": location.ymax,
                },
            }
            for slug, location in config.locations.items()
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    cached_locations = list(weather_service.cached_forecasts.keys())
    return {
        "status": "healthy",
        "cached_locations": cached_locations,
        "total_locations": len(config.locations),
        "refresh_interval_minutes": config.server.refresh_interval_minutes,
        "cache_status": {
            slug: forecast.last_updated.isoformat()
            for slug, forecast in weather_service.cached_forecasts.items()
        },
    }


@app.post("/refresh/{slug}")
async def refresh_location(slug: str):
    """Manually refresh weather data for a specific location."""
    if slug not in config.locations:
        raise HTTPException(status_code=404, detail=f"Location '{slug}' not found")

    try:
        location = config.locations[slug]
        await update_task.update_location(slug, location, None)
        return {"message": f"Successfully refreshed data for {slug}"}
    except Exception as e:
        logger.error(f"Failed to refresh {slug}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to refresh data for {slug}: {str(e)}"
        )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Handle general exceptions."""
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500, content={"error": "Internal server error", "message": str(exc)}
    )


if __name__ == "__main__":
    import uvicorn

    # Get configuration from environment or config
    host = os.getenv("HOST", config.server.host)
    port = int(os.getenv("PORT", config.server.port))
    workers = int(os.getenv("WORKERS", 1))

    logger.info(f"Starting server on {host}:{port}")

    uvicorn.run(app, host=host, port=port, log_level=log_level.lower(), access_log=True)
