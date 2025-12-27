"""Package responsible for log configs."""

from src.app.system.logs.config import get_logger
from src.app.system.logs.setup import setup_logging

__all__ = ["get_logger", "setup_logging"]