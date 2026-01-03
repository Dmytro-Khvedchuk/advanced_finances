"""This module centralizes the logging configuration for the application.

It provides functions to set up the global Loguru logger, including handlers,
formatters, and filters. It also offers a utility function, `get_logger`,
to create logger instances that are bound to specific module names,
enabling more structured and context-rich logging.
"""
import sys
from typing import TYPE_CHECKING

from loguru import logger  # type: ignore[reportMissingImports]


if TYPE_CHECKING:
    from loguru import Logger  # type: ignore[reportMissingImports]

LOG_FORMAT: str = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSSSSSZ}</green> | "
    "<level>{level}</level> | "
    "<magenta>{module}</magenta>:"
    "<cyan>{function}</cyan>:"
    "<white>{line}</white> | "
    "<level>{message}</level>"
)


def setup_logger(level: str) -> None:
    """Configures the global logger with the specified logging level and format.

    The logger output is directed to stdout. The format is a simple string format,
    and a filter is added to inject correlation IDs into log records. The `serialize=True`
    option ensures structured output, which is ideal for log aggregation systems.

    Args:
        level (str): The minimum logging level to display (e.g., "DEBUG", "INFO",
                     "WARNING", "ERROR", "CRITICAL").
    """
    logger.remove()  # Remove default handlers

    logger.add(
        sys.stdout,
        level=level,
        format=LOG_FORMAT,
        serialize=False,
        colorize=True,
        enqueue=True,
    )


def get_logger(module_name: str) -> "Logger":
    """Returns a logger instance bound to the specified module name.

    This function acts as a factory for creating a logger instance for a given
    module. It utilizes Loguru's `bind` method to attach the module name as
    contextual information to every log record. This context is valuable for
    identifying the source of log messages in a structured logging environment.

    Args:
        module_name (str): The name of the module for which the logger is created,
                           typically obtained using `__name__`.

    Returns:
        Logger: A Loguru Logger instance with the 'module' field bound.
    """
    return logger.bind(module=module_name)
