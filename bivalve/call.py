# --------------------------------------------------------------------
# call.py
#
# Author: Lain Musgrove (lain.musgrove@hearst.com)
# Date: Thursday August 10, 2023
# --------------------------------------------------------------------

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum, auto
from io import StringIO

from bivalve.datatypes import ArgV, ThreadAtomicCounter
from bivalve.util import new_future, str_escape

# --------------------------------------------------------------------
CALL_AUTO_INCREMENT = ThreadAtomicCounter()


# --------------------------------------------------------------------
@dataclass
class Response:
    code: "Response.Code"
    content: ArgV
    dt: datetime = field(default_factory=datetime.now)

    class Code(StrEnum):
        OK = auto()
        NOT_FOUND = auto()
        ERROR = auto()

    class Errors(StrEnum):
        UNDEFINED_FUNCTION = auto()
        RUNTIME_ERROR = auto()


# --------------------------------------------------------------------
@dataclass
class Call:
    function: str
    params: ArgV
    future: asyncio.Future[Response] = field(default_factory=new_future)
    expires_at: datetime = datetime.max
    id: int = field(default_factory=CALL_AUTO_INCREMENT.next)

    def to_call_cmd_argv(self) -> ArgV:
        return ["call", str(self.id), self.function, *[str(p) for p in self.params]]

    def __str__(self):
        sb = StringIO()
        sb.write(f"{self.function}(")
        sb.write(", ".join([f'"{str_escape(p)}"' for p in self.params]))
        sb.write(")")
        return sb.getvalue()

    def __repr__(self):
        sb = StringIO()
        sb.write(f"<{self.__class__.__qualname__} ")
        sb.write(str(self))
        sb.write(f" id={self.id}")
        sb.write(">")

    def __await__(self):
        return self.future.__await__()


# --------------------------------------------------------------------
class CallFailedError(Exception):
    def __init__(self, call: Call, details: ArgV):
        super().__init__(f"Call to agent function `{call}` failed.")
        self.call = call
        self.details = details
