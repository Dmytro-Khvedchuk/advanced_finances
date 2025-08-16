from logging import Formatter, INFO, getLogger, StreamHandler
from colorama import init, Fore, Style

init(autoreset=True)


class ColoredFormatter(Formatter):
    COLORS = {
        "DEBUG": Fore.LIGHTBLACK_EX,
        "INFO": Fore.BLUE,
        "WARNING": Fore.YELLOW,
        "ERROR": Fore.RED,
        "CRITICAL": Fore.MAGENTA,
    }

    def format(self, record):
        color = self.COLORS.get(record.levelname, "")
        message = super().format(record)
        return f"{color}{message}{Style.RESET_ALL}"


class LoggerWrapper:
    """
    A simple wrapper for the events, with colored output.
    """

    def __init__(self, name: str, level: int = INFO):
        self.logger = getLogger(name)
        self.logger.setLevel(level)
        if not self.logger.hasHandlers():
            handler = StreamHandler()
            formatter = ColoredFormatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def info(self, message: str):
        """Log an info message"""
        self.logger.info(message)

    def error(self, message: str):
        """Log an error message"""
        self.logger.error(message)

    def warning(self, message: str):
        """Log a warning message"""
        self.logger.warning(message)

    def debug(self, message: str):
        """Log a debug message"""
        self.logger.debug(message)
