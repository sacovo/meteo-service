import toml
from models import Config, LocationConfig, ServerConfig


def load_config(config_path: str = "config.toml") -> Config:
    """Load configuration from TOML file."""
    try:
        with open(config_path, "r") as f:
            config_data = toml.load(f)

        # Parse server config
        server_data = config_data.get("server", {})
        server_config = ServerConfig(**server_data)

        # Parse locations
        locations_data = config_data.get("locations", {})
        locations = {}

        for slug, location_data in locations_data.items():
            locations[slug] = LocationConfig(**location_data)

        return Config(server=server_config, locations=locations)

    except Exception as e:
        raise Exception(f"Failed to load config: {e}")


def get_location_slugs(config: Config) -> list[str]:
    """Get list of all configured location slugs."""
    return list(config.locations.keys())
