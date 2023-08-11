# --------------------------------------------------------------------
# call.py
#
# Author: Lain Musgrove (lain.musgrove@hearst.com)
# Date: Thursday August 10, 2023
# --------------------------------------------------------------------

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum, auto

from bivalve.datatypes import ArgV, AtomicResult, BaseID, id_to_str, new_id


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
    ID = BaseID
    function: str
    params: ArgV
    expires_at: datetime = datetime.max
    id: ID = field(default_factory=new_id)
    response: AtomicResult[Response] = AtomicResult()

    @property
    def sid(self):
        return id_to_str(self.id)

    def to_argv(self) -> ArgV:
        return ["call", self.id, *[str(p) for p in self.params]]


# --------------------------------------------------------------------
class CallFailedError(Exception):
    def __init__(self, call: Call, details: ArgV):
        super().__init__(f"Call to agent function `{call.function}` failed.")
        self.call = call
        self.details = details
