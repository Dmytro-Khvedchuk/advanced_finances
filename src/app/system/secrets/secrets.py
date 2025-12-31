"""A file responsible for the Secret instance, it provides the single source of truth for the system."""

import os


class Secrets:
    """Class that contains all of the secrets and environmental variables."""
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
