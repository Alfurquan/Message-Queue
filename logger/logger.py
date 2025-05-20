import logging

from abc import ABC, abstractmethod
from config.config import get_log_config

class Logger(ABC):
    """
    Abstract base class for logging.
    """
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        config = get_log_config()
        self.logger.setLevel(config.get("log_level"))
        self.logger.propagate = False

        if not self.logger.handlers:
            handler = self._get_handler()
            formatter = logging.Formatter(config.get("console_format"), datefmt=config.get("date_format"))
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    @abstractmethod
    def _get_handler(self) -> logging.Handler:
        pass

    def get_logger(self) -> logging.Logger:
        return self.logger

    