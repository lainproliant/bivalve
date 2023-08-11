# --------------------------------------------------------------------
# datatypes.py
#
# Author: Lain Musgrove (lain.musgrove@gmail.com)
# Date: Monday July 31, 2023
# --------------------------------------------------------------------

import asyncio
from typing import Callable, Generic, Optional, TypeVar
from uuid import UUID, uuid4

import shortuuid

from bivalve.util import get_millis

# --------------------------------------------------------------------
T = TypeVar("T")
ArgV = list[str]
ArgVQueue = asyncio.Queue[ArgV]
BaseID = UUID


# --------------------------------------------------------------------
def new_id() -> BaseID:
    return uuid4()


# --------------------------------------------------------------------
def id_to_str(id: BaseID) -> str:
    return shortuuid.encode(id)


# --------------------------------------------------------------------
def str_to_id(s: str) -> BaseID:
    return shortuuid.decode(s)


# --------------------------------------------------------------------
class AtomicValue(Generic[T]):
    """
    A value with methods to support fetching and acting upon it atomically
    across multiple async coroutines.
    """

    def __init__(self, value: T):
        self.value = value
        self.lock = asyncio.Lock()

    async def __call__(self):
        async with self.lock:
            return self.value

    async def set(self, value: T):
        async with self.lock:
            self.value = value

    async def mutate(self, mutator: Callable[[T], T]):
        async with self.lock:
            self.value = mutator(self.value)


# --------------------------------------------------------------------
class AtomicCounter:
    """
    An atomic auto-incrementing counter starting at 0.
    """

    def __init__(self, value: T):
        self.value = -1
        self.lock = asyncio.Lock()

    async def __call__(self):
        async with self.lock:
            self.value += 1
            return self.value


# --------------------------------------------------------------------
class AtomicResult(Generic[T], AtomicValue[Optional[T]]):
    def __init__(self):
        super().__init__(None)

    async def has_result(self):
        value = await super().__call__()
        return value is not None

    async def __call__(self, sleep_ms=50, timeout_ms=0):
        start_ms = get_millis()
        while not await self.has_result():
            await asyncio.sleep(sleep_ms / 1000)
            if timeout_ms > 0 and (get_millis() - start_ms) >= timeout_ms:
                raise TimeoutError(
                    f"Timed out after {timeout_ms}ms waiting for AtomicResult."
                )
        return await super().__call__()
