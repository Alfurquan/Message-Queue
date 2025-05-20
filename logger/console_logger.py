from .logger import Logger
import logging

class ConsoleLogger(Logger):
    """
    Console logger implementation.
    """
    def _get_handler(self) -> logging.Handler:
        """
        Returns a console handler for logging.
        """
        return logging.StreamHandler()