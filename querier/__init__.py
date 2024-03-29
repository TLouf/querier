import logging
from logging.handlers import RotatingFileHandler

from querier.connection import CollectionsAccessor, Connection, MongoGroupBy, NamedAgg
from querier.exceptions import (
    AuthentificationError,
    CredentialsError,
    InternalError,
    InvalidFilter,
    ServerError,
)
from querier.filter import Filter
from querier.result import Result

__all__ = [
    "Connection",
    "Result",
    "Filter",
    "MongoGroupBy",
    "CollectionsAccessor",
    "NamedAgg",
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

    file_formatter = logging.Formatter("[%(levelname)s] <%(asctime)s>: %(message)s")

    max_size = 256 * 1024
    fh = RotatingFileHandler(
        filename, backupCount=1, maxBytes=max_size, encoding="utf-8"
    )
    fh.setFormatter(file_formatter)

    logger.addHandler(fh)
    return logger


init_logger("querier", "querier.log")
