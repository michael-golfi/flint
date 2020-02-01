"""
    logging.py contains functions for interacting with the logging
"""

import logging


def get_logger(name: str) -> logging.Logger:
    """
    Get an instance of a logger with configured format and level
    """
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level="INFO")
    return logging.getLogger(name)
