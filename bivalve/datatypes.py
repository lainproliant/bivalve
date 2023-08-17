# --------------------------------------------------------------------
# datatypes.py
#
# Author: Lain Musgrove (lain.musgrove@gmail.com)
# Date: Monday July 31, 2023
# --------------------------------------------------------------------

import asyncio
import threading
from typing import Callable, Generic, Optional, TypeVar

# --------------------------------------------------------------------
T = TypeVar("T")
ArgV = list[str]
ArgVQueue = asyncio.Queue[ArgV]


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
class AtomicBox(Generic[T]):
    """
    A single-item container with methods to support fetching and updating the
    stored contents atomically in an async context.
    """

    def __init__(self):
        self._value: AtomicValue[Optional[T]] = AtomicValue(None)

    async def is_empty(self) -> bool:
        value = await self._value()
        return value is None

    async def get(self) -> T:
        value = await self._value()
        if value is None:
            raise ValueError("No value has been supplied yet.")
        return value

    async def set(self, value: T):
        value = await self._value.set(value)

    async def clear(self):
        await self._value.set(None)


# --------------------------------------------------------------------
class ThreadAtomicCounter:
    """
    A thread-safe atomic auto-incrementing counter starting at 0,
    yielding numbers starting at 1 via `next()`.
    """

    def __init__(self):
        self.value = 0
        self.lock = threading.Lock()

    def next(self):
        with self.lock:
            self.value += 1
            return self.value
