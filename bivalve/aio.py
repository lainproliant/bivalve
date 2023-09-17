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
from io import StringIO
from pathlib import Path
from ssl import SSLContext
from typing import Optional

from bivalve.datatypes import (
    ArgV,
    ArgVQueue,
    AtomicValue,
    ThreadAtomicCounter,
)
from bivalve.logging import LogManager

# --------------------------------------------------------------------
log = LogManager().get(__name__)

SERVER_AUTO_ID = ThreadAtomicCounter()
STREAM_AUTO_ID = ThreadAtomicCounter()

# --------------------------------------------------------------------
@dataclass
class SocketParams:
    """
    Encapsulates and validates the range of parameters available
    when connecting to or starting a server via sockets.
    """

    host: Optional[str] = None
    port: Optional[int] = None
    path: Optional[Path | str] = None
    ssl: Optional[SSLContext] = None

    def __post_init__(self):
        if not self._validate():
            raise ValueError("Invalid socket params.")

    def _validate(self) -> bool:
        if self.host and self.port:
            return True
        elif self.path:
            return True
        return False

    @property
    def is_tcp(self):
        return self.host is not None

    @property
    def is_unix_path(self):
        return self.path is not None

    def __str__(self):
        sb = StringIO()

        if self.host and self.port:
            sb.write(f"{self.host}:{self.port}")
        elif self.path:
            sb.write(f"file={self.path}")
        else:
            sb.write("INVALID")

        return sb.getvalue()

    def __repr__(self):
        sb = StringIO()
        sb.write(f"<{self.__class__.__qualname__} ")
        sb.write(str(self))
        sb.write(">")
        return sb.getvalue()


# --------------------------------------------------------------------
@dataclass
class Server:
    """
    Class encapsulating an asyncio.Server and the connection details
    that were used to establish it.
    """

    params: SocketParams
    asyncio_server: asyncio.Server
    id: int = field(default_factory=SERVER_AUTO_ID.next)

    @classmethod
    def _wrap_callback(self, params: SocketParams, callback):
        async def connected_callback(reader, writer):
            stream = Stream(reader, writer, params)
            if inspect.iscoroutinefunction(callback):
                await callback(stream)
            else:
                callback(stream)

        return connected_callback

    @classmethod
    async def serve(cls, callback, **kwargs) -> "Server":
        """
        Used to start a server on a TCP port or UNIX named socket path which
        will be fed Stream objects for connected clients via the provided
        `callback` function or coroutine.
        """

        params = SocketParams(**kwargs)
        callback = cls._wrap_callback(params, callback)
        if params.is_tcp:
            asyncio_server = await asyncio.start_server(
                callback, host=params.host, port=params.port, ssl=params.ssl
            )

        else:  # if params.is_unix_path
            asyncio_server = await asyncio.start_unix_server(
                callback, path=params.path, ssl=params.ssl
            )

        return Server(params, asyncio_server)

    def close(self):
        self.asyncio_server.close()

    def __repr__(self):
        sb = StringIO()
        sb.write(f"<{self.__class__.__qualname__} ")
        sb.write(f"id={self.id} ")
        sb.write(f"{self.params}")
        sb.write(">")
        return sb.getvalue()


# --------------------------------------------------------------------
@dataclass
class Stream:
    """
    Class encapsulating an asyncio StreamReader/StreamWriter pair
    for an open connection and the params used to establish it.
    """

    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    params: SocketParams
    id: int = field(default_factory=STREAM_AUTO_ID.next)

    async def close(self):
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception:
            log.exception(f"Failed to close stream: {self}.")

    @classmethod
    async def connect(cls, **kwargs) -> "Stream":
        params = SocketParams(**kwargs)
        if params.is_tcp:
            reader, writer = await asyncio.open_connection(
                params.host, params.port, ssl=params.ssl
            )
        else:  # if params.is_unix_path
            reader, writer = await asyncio.open_unix_connection(
                params.path, ssl=params.ssl
            )

        return Stream(reader, writer, params)

    def __repr__(self):
        sb = StringIO()
        sb.write(f"<{self.__class__.__qualname__} ")
        sb.write(f"id={self.id} ")
        sb.write(f"{self.params}")
        sb.write(">")
        return sb.getvalue()


# --------------------------------------------------------------------
class Connection:
    """
    Abstract base type for connections which send and receive
    shell-style commands.
    """

    def __init__(self, id: int):
        self.id = id
        self.alive = AtomicValue(True)

    @classmethod
    async def connect(cls, **kwargs) -> "Connection":
        stream = Stream.connect(**kwargs)
        return StreamConnection(stream)

    @classmethod
    def bridge(
        cls, send_queue: ArgVQueue, recv_queue: ArgVQueue, poll_timeout=1.0
    ) -> "Connection":
        return BridgeConnection(send_queue, recv_queue, poll_timeout)

    async def close(self):
        if await self.alive():
            await self.alive.set(False)

    async def send(self, *argv):
        assert argv
        assert len(argv) > 0
        argv = [*argv]
        await self._send(*argv)

        log.debug(f"Sent {argv} to {self}")

    async def recv(self) -> ArgV:
        argv = await self._recv()
        assert argv
        log.debug(f"Received {argv} from {self}")
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

    async def _recv(self) -> ArgV:
        raise NotImplementedError()

    async def __aenter__(self) -> "Connection":
        return self

    async def __aexit__(self, exc_t, exc_v, exc_tb):
        await self.try_send("bye")
        await self.close()


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

    async def _recv(self) -> ArgV:
        out = await self.stream.reader.readline()
        if not out or not await self.alive():
            raise ConnectionAbortedError()
        return shlex.split(out.decode())

    async def _send(self, *argv):
        self.stream.writer.write((shlex.join([str(s) for s in argv]) + "\n").encode())
        await self.stream.writer.drain()

    def __repr__(self):
        return repr(self.stream)
        sb = StringIO()
        sb.write(f"<{self.__class__.__qualname__} ")
        sb.write(f"{self.stream.params}")
        sb.write(">")
        return sb.getvalue()


# --------------------------------------------------------------------
class BridgeConnection(Connection):
    """
    Connection using queues to send and receive shell-style commands.

    Used to connect to a BivalveAgent in the same process, or to bridge a
    BivalveAgent connection across another medium.
    """

    def __init__(
        self,
        send_queue: ArgVQueue,
        recv_queue: ArgVQueue,
        poll_timeout: float = 1.0,
    ):
        super().__init__(STREAM_AUTO_ID.next())
        self.send_queue = send_queue
        self.recv_queue = recv_queue
        self.poll_timeout = poll_timeout

    async def _recv(self) -> ArgV:
        while await self.alive():
            result = await self.recv_queue.get()
            self.recv_queue.task_done()
            return result
        raise ConnectionAbortedError()

    async def _send(self, *argv):
        await self.send_queue.put(argv)
