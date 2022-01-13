import logging
import logging.handlers

from .filter import Filter
from .result import Result
from .connection import Connection
from .exceptions import (
    InvalidFilter,
    CredentialsError,
    AuthentificationError,
    ServerError,
    InternalError,
)

__all__ = [
    "Connection", 
    "Result",
    "Filter",
    "InvalidFilter",
    "CredentialsError",
    "AuthentificationError",
    "ServerError",
    "InternalError",
]
__version__ = "0.0.5"

def init_logger(name, filename, level=logging.DEBUG):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    file_formatter = logging.Formatter('[%(levelname)s] <%(asctime)s>: %(message)s')

    max_size = 256 * 1024
    fh = logging.handlers.RotatingFileHandler(filename, maxBytes=max_size)
    fh.setFormatter(file_formatter)

    logger.addHandler(fh)
    return logger

init_logger("querier", "querier.log")
