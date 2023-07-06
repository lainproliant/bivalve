# --------------------------------------------------------------------
# call.py
#
# Author: Lain Musgrove (lain.proliant@gmail.com)
# Date: Tuesday May 9, 2023
#
# Distributed under terms of the MIT license.
# --------------------------------------------------------------------

import asyncio
from uuid import UUID, uuid4
from datetime import datetime, timedelta

from typing import Optional
from bivalve.constants import CallFailureTypes


# --------------------------------------------------------------------
class Call:
    ID = UUID
    DEFAULT_TIMEOUT = timedelta(seconds=30)

    def __init__(self, fn: str, *argv: str, timeout: timedelta = DEFAULT_TIMEOUT):
        self.id = uuid4()
        self.fn = fn
        self.argv = argv
        self.timeout = timeout
        self.created_at = datetime.now()
        self.returned_at: Optional[datetime] = None
        self.failed_at: Optional[datetime] = None
        loop = asyncio.get_running_loop()
        self._future: asyncio.Future[list[str]] = loop.create_future()

    def set_result(self, result):
        if self.is_done():
            raise ValueError("Call is already done.")
        if self.is_failed():
            raise ValueError("Call has already failed.")
        if self.is_expired():
            raise ValueError("Call is already expired.")

        self._future.set_result(result)
        self.returned_at = datetime.now()

    def set_exception(self, exc):
        if self.is_expired():
            return

        self._future.set_exception(exc)
        self.failed_at = datetime.now()

    def is_done(self):
        return self.returned_at is not None

    def is_failed(self):
        return self.failed_at is not None

    async def wait_for_result(self):
        try:
            async with asyncio.timeout(self.timeout.total_seconds()):
                return await self._future

        except TimeoutError:
            raise CallTimeout(self)

    def is_expired(self) -> bool:
        now = datetime.now()
        return self.returned_at is not None and (now - self.created_at) > self.timeout


# --------------------------------------------------------------------
class CallFailed(Exception):
    def __init__(self, call: Call, err_val: str):
        super().__init__(
            f"Call `{call.id}` of `{call.fn}` function failed: `{err_val}`"
        )
        self.call = call
        self.err_val = err_val


# --------------------------------------------------------------------
class CallTimeout(CallFailed):
    def __init__(self, call: Call):
        super().__init__(call, CallFailureTypes.TIMEOUT)
