# CLAUDE.md - Guide for Working with meteo-service

## Build & Run Commands
- Install dependencies: `pip install -r requirements.txt`
- Run service: `python main.py`
- Run with Docker: `docker compose up -d`
- Lint code: `ruff check .`
- Type check: `mypy .`

## Code Style Guidelines
- Python 3.12+ compatible code
- PEP 8 style guidelines with 88 character line limit
- Type hints required for all function parameters and return values
- Imports grouped by standard library, third-party, and local modules
- Use f-strings for string formatting
- Async/await for I/O operations
- Proper error handling with specific exception types
- Clear logging with appropriate log levels
- Pydantic models for data validation
- Docker-friendly design (stateless, environment variables)

## Naming Conventions
- Classes: PascalCase
- Functions/methods: snake_case
- Variables: snake_case
- Constants: UPPER_SNAKE_CASE
- File names: snake_case.py