"""This module orchestrates the complete logging setup for the application.

It acts as the central point for configuring both the Loguru logger and
the standard Python `logging` module. The primary goal is to ensure that
all logs, whether from the application itself or from third-party libraries,
are captured, processed, and formatted consistently by Loguru.
"""
import logging

from src.app.system.logs.config import get_logger, setup_logger
from src.app.system.secrets import Secrets


def setup_logging() -> None:
    """Configures the application's entire logging system.

    This function performs a three-step process to ensure a consistent and
    robust logging environment:

    1.  **Loguru Configuration:** It initializes the Loguru logger with a specified
        level, handling, and format. This is the primary logger used by the application.

    2.  **Standard Library Interception:** It sets up the root standard library
        logger to use an `InterceptHandler`. This redirects all log messages
        that use the standard `logging` module to the Loguru logger, ensuring
        no logs are missed.

    3.  **Third-Party Logger Overriding:** It explicitly iterates through all
        currently defined standard library loggers (e.g., from third-party
        libraries) and assigns the `InterceptHandler` to them. It also sets
        their `propagate` attribute to `False` to prevent duplicate log entries,
        as many libraries configure their loggers in a way that bypasses
        simple root logger inheritance.
    """
    # Step 1: Configure the global Loguru logger
    setup_logger(level=Secrets.log_level)  # TODO: add the log level based on the deploy variable
    # Bind the main logger to a module name. This is a good practice for structured logging.
    get_logger(module_name="__main__")

    # Step 2: Configure the root standard library logger to use our InterceptHandler
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Step 3: Explicitly apply the InterceptHandler to specific third-party loggers
    listed_loggers: list[str] = [
        logging.getLogger(name).name
        for name in logging.root.manager.loggerDict
    ]

    for logger_name in set(listed_loggers):
        log: logging.Logger = logging.getLogger(logger_name)
        log.propagate = False

    # Suppress specific noisy loggers if needed.
    # Example: Passlib attempting to read a version of bcrypt can cause warnings/errors.
    logging.getLogger("passlib").setLevel(logging.ERROR)
