"""
db_config.py
------------
PostgreSQL connection settings.

Reads values from environment variables.
Locally, these are loaded from your .env file (via python-dotenv).
In Docker, they're set in docker-compose.yml.

Setup:
  cp .env.example .env
  # Fill in your values in .env
"""

import os
from pathlib import Path

# Load .env file if it exists (does nothing in Docker where env vars are already set)
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
except ImportError:
    pass  # python-dotenv not installed — rely on env vars being set manually

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST",     "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB",       "ecommerce"),
    "user":     os.getenv("POSTGRES_USER",     "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}
