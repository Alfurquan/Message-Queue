"""
Configuration module for the logger service.
"""
import os

# Log levels with corresponding numeric values
LOG_LEVELS = {
    "DEBUG": 10,
    "INFO": 20,
    "WARNING": 30,
    "ERROR": 40,
    "CRITICAL": 50
}

# Default configuration values
DEFAULT_LOG_CONFIG = {
    # Log level (can be overridden by environment variable)
    "log_level": os.environ.get("LOG_LEVEL", "INFO"),
    
    # Log file settings
    "log_to_file": os.environ.get("LOG_TO_FILE", "false").lower() == "true",
    "log_file_path": os.environ.get("LOG_FILE_PATH", "/logs/logger.log"),
    "log_max_size_mb": float(os.environ.get("LOG_MAX_SIZE_MB", "1.0")),
    
    # Format strings for different log destinations
    "console_format": "%(asctime)s [%(levelname)s] [%(funcName)s] %(message)s",
    "date_format": "%Y-%m-%dT%H:%M:%S%z",
    "file_format": "{timestamp} | {level} | {message}",
}

def get_log_config():
    """
    Returns the current log configuration.
    """
    return DEFAULT_LOG_CONFIG

def get_log_level_value(level_name):
    """Converts a log level name to its numeric value."""
    return LOG_LEVELS.get(level_name.upper(), LOG_LEVELS["INFO"])

def is_log_enabled(message_level, config_level):
    """Determines if a message should be logged based on the configured level."""
    return get_log_level_value(message_level) >= get_log_level_value(config_level)