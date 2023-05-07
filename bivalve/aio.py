# --------------------------------------------------------------------
# async.py
#
# Author: Lain Musgrove (lain.proliant@gmail.com)
# Date: Thursday February 16, 2023
#
# Distributed under terms of the MIT license.
# --------------------------------------------------------------------

import asyncio
import inspect
import shlex
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Generic, Optional, TypeVar
from uuid import UUID, uuid4

from bivalve.logging import LogManager

# --------------------------------------------------------------------
log = LogManager().get(__name__)
BaseID = UUID

T = TypeVar("T")


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
@dataclass
class Stream:
    """
    Class encapsulating an asyncio StreamReader/StreamWriter pair
    for an open connection.
    """

    ID = BaseID
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    id: UUID = field(default_factory=uuid4)

    async def close(self):
        self.writer.close()
        await self.writer.wait_closed()

    @staticmethod
    async def connect(host: str, port: int, ssl=None) -> "Stream":
        reader, writer = await asyncio.open_connection(host, port, ssl=ssl)
        return Stream(reader, writer)

    @staticmethod
    async def start_server(callback, host: str, port: int, ssl=None) -> asyncio.Server:
        """
        Used to start a server which will be fed Stream objects for connected
        clients via the provided `callback` function or coroutine.
        """

        async def connected_callback(reader, writer):
            if inspect.iscoroutinefunction(callback):
                await callback(Stream(reader, writer))
            else:
                callback(Stream(reader, writer))

        return await asyncio.start_server(connected_callback, host, port, ssl=ssl)


# --------------------------------------------------------------------
class Connection:
    """
    Abstract base type for connections which send and receive
    shell-style commands.
    """

    ID = BaseID

    def __init__(self, id: ID):
        self.id = id
        self.alive = AtomicValue(True)

    async def close(self):
        if await self.alive():
            await self.alive.set(False)

    async def send(self, *argv):
        assert argv
        assert len(argv) > 0
        await self._send(*argv)

        log.debug(f"Sent `{shlex.join(argv)}` to {self.id}")

    async def recv(self) -> list[str]:
        argv = await self._recv()
        assert argv
        log.debug(f"Received `{argv}` from {self.id}")
        return argv

    async def try_send(self, *argv):
        assert argv
        assert len(argv) > 0

        try:
            await self.send(*argv)

        except ConnectionError:
            log.warning(f"Could not send `{argv[0]}`, connection was lost.")

        except Exception as e:
            log.warning(f"Could not send `{argv[0]}`, unexpected error occurred.", e)

    async def _send(self, *argv):
        raise NotImplementedError()

    async def _recv(self) -> list[str]:
        raise NotImplementedError()


# --------------------------------------------------------------------
class StreamConnection(Connection):
    """
    Connection using a Stream to send and receive shell-style commands.

    Used to connect to a BivalveAgent over a socket connection.
    """

    def __init__(self, stream: Stream):
        super().__init__(stream.id)
        self.stream = stream
        self.syn_at = datetime.min
        self.ack_ttl: Optional[datetime] = None

    async def close(self):
        if await self.alive():
            await self.stream.close()
            await self.alive.set(False)

    async def _recv(self) -> list[str]:
        out = await self.stream.reader.readline()
        if not out or not await self.alive():
            raise ConnectionAbortedError()
        return shlex.split(out.decode())

    async def _send(self, *argv):
        self.stream.writer.write((shlex.join([str(s) for s in argv]) + "\n").encode())
        await self.stream.writer.drain()


# --------------------------------------------------------------------
class BridgeConnection(Connection):
    """
    Connection using queues to send and receive shell-style commands.

    Used to connect to a BivalveAgent in the same process, or to bridge a
    BivalveAgent connection across another medium.
    """

    def __init__(
        self,
        send_queue: asyncio.Queue[str],
        recv_queue: asyncio.Queue[str],
        poll_timeout: float = 1.0,
    ):
        super().__init__(uuid4())
        self.send_queue = send_queue
        self.recv_queue = recv_queue
        self.poll_timeout = poll_timeout

    async def _recv(self) -> list[str]:
        while await self.alive():
            result = await self.recv_queue.get()
            self.recv_queue.task_done()
            return result
        raise ConnectionAbortedError()

    async def _send(self, *argv):
        await self.send_queue.put(argv)
