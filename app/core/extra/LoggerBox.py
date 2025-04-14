import logging
import os
from pathlib import Path


class LoggerBox:
    # PRIVATE
    _instance = None
    _loggers = {}

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(LoggerBox, cls).__new__(cls)
        return cls._instance

    def __init__(self, dir="./log"):
        if not hasattr(self, "_initialized"):
            self._dir = dir
            Path(self._dir).mkdir(parents=True, exist_ok=True)
            self._initialized = True

    def _init_logger(self, name):  # only called once
        # target
        logger = logging.getLogger(name)

        # only remove target logger handler (extra exception case called more than twice...)
        handlers_to_remove = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]
        for h in handlers_to_remove:
            logger.removeHandler(h)

        # set the logger
        logger.setLevel(logging.DEBUG)

        # info set
        info_log_path = os.path.join(self._dir, f"info_{name}.log")
        info_file_handler = logging.FileHandler(info_log_path)
        info_file_handler.setLevel(logging.INFO)
        info_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        info_file_handler.setFormatter(info_formatter)
        info_file_handler.addFilter(lambda record: record.levelno < logging.ERROR)

        # error set
        error_log_path = os.path.join(self._dir, f"error_{name}.log")
        error_file_handler = logging.FileHandler(error_log_path)
        error_file_handler.setLevel(logging.ERROR)
        error_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        error_file_handler.setFormatter(error_formatter)

        # return
        logger.addHandler(info_file_handler)
        logger.addHandler(error_file_handler)
        return logger

    # PUBLIC
    def get_logger(self, name):
        if name not in self._loggers:
            self._loggers[name] = self._init_logger(name)
        return self._loggers[name]
