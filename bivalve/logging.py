# --------------------------------------------------------------------
# logging.py
#
# Author: Lain Musgrove (lain.proliant@gmail.com)
# Date: Thursday February 16, 2023
#
# Distributed under terms of the MIT license.
# --------------------------------------------------------------------
import itertools
import logging
from typing import Optional

from bivalve.util import Borg


# --------------------------------------------------------------------
class LogManager(Borg):
    """
    A borg singleton allowing for globally managed log handlers.
    """

    def __init__(self):
        super().__init__()
        if hasattr(self, "_initialized"):
            return

        self._filename: Optional[str] = None
        self._format = "%(asctime)s [%(levelname)s]: %(message)s"
        self._level = logging.INFO
        self._handlers: list[logging.Handler] = []
        self._special_handlers: list[logging.Handler] = []
        self._has_console_handler = False
        self._logs: dict[str, logging.Logger] = {}

        root = logging.getLogger()
        root.setLevel(self._level)
        self._logs["__root__"] = root
        self._initialized = True

    def _create(self, name) -> logging.Logger:
        log = logging.Logger(name)
        for handler in self.handlers():
            log.addHandler(handler)
        log.setLevel(self._level)
        self._logs[name] = log
        return log

    def handlers(self):
        return itertools.chain(self._handlers, self._special_handlers)

    def setup(self):
        """
        Optional setup method to add the default console handler.
        """
        if not self._has_console_handler:
            self.add_handler(logging.StreamHandler())
            self._has_console_handler = True

    def add_handler(self, handler: logging.Handler):
        """
        Add a handler that will adopt the shared level and format properties,
        and will be updated when they change.
        """
        self._handlers.append(handler)
        handler.setLevel(self._level)
        handler.setFormatter(logging.Formatter(self._format))
        for log in self._logs.values():
            log.addHandler(handler)

    def add_special_handler(self, handler: logging.Handler):
        """
        Add a handler with special properties that won't be overwritten
        when shared properties such as level and format are changed.
        """
        self._special_handlers.append(handler)
        for log in self._logs.values():
            log.addHandler(handler)

    def set_format(self, fmt: str):
        self._format = fmt
        for handler in self._handlers:
            handler.setFormatter(logging.Formatter(self._format))

    def set_level(self, level):
        self._level = level
        for handler in self._handlers:
            handler.setLevel(level)
        for log in self._logs.values():
            log.setLevel(level)

    def get(self, name) -> logging.Logger:
        if name in self._logs:
            return self._logs[name]
        return self._create(name)
