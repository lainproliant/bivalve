# --------------------------------------------------------------------
# util.py
#
# Author: Lain Musgrove (lain.proliant@gmail.com)
# Date: Saturday February 11, 2023
#
# Distributed under terms of the MIT license.
# --------------------------------------------------------------------

import asyncio
import fcntl
import inspect
import os
import selectors
import sys
from dataclasses import dataclass, field
from typing import Any, Optional, TextIO
from uuid import UUID, uuid4


# --------------------------------------------------------------------
class Borg:
    _shared_state = {}

    def __init__(self):
        self.__dict__ = self._shared_state


# --------------------------------------------------------------------
def get_prefixed_methods(prefix, obj):
    return [
        getattr(obj, name)
        for name in dir(obj)
        if name.startswith(prefix) and callable(getattr(obj, name))
    ]


# --------------------------------------------------------------------
class Commands:
    @staticmethod
    def get_prefixed_methods(prefix, obj):
        return [
            getattr(obj, name)
            for name in dir(obj)
            if name.startswith(prefix) and callable(getattr(obj, name))
        ]

    def __init__(self, obj, prefix="cmd_"):
        self.map: dict[str, Any] = {}
        self.prefix = prefix
        for method in Commands.get_prefixed_methods(self.prefix, obj):
            self.define(method)

    def define(self, f):
        self.map[f.__name__.removeprefix(self.prefix)] = f

    def get(self, name) -> Any:
        if name not in self.map:
            raise ValueError(f"Command `{name}` is not defined.")
        return self.map[name]

    def list(self):
        return sorted(self.map.keys())

    def signatures(self):
        for key, value in sorted(self.map.items(), key=lambda x: x[0]):
            yield (key, inspect.signature(value))
