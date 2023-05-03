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
from typing import Optional
from uuid import UUID, uuid4

from bivalve.logging import LogManager

# --------------------------------------------------------------------
log = LogManager().get(__name__)


# --------------------------------------------------------------------
@dataclass
class Stream:
    ID = UUID
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    id: UUID = field(default_factory=uuid4)

    async def close(self):
        self.writer.close()
        await self.writer.wait_closed()

    @staticmethod
    async def connect(host: str, port: int, ssl=None):
        reader, writer = await asyncio.open_connection(host, port, ssl=ssl)
        return Stream(reader, writer)

    @staticmethod
    async def start_server(callback, host: str, port: int, ssl=None):
        async def connected_callback(reader, writer):
            if inspect.iscoroutinefunction(callback):
                await callback(Stream(reader, writer))
            else:
                callback(Stream(reader, writer))

        return await asyncio.start_server(connected_callback, host, port, ssl=ssl)


# --------------------------------------------------------------------
@dataclass
class Connection:
    ID = Stream.ID
    stream: Stream
    task: asyncio.Task
    syn_at: datetime = datetime.min
    ack_ttl: Optional[datetime] = None
    alive: bool = True

    @property
    def id(self):
        return self.stream.id

    async def close(self):
        if self.alive:
            await self.stream.close()
            self.alive = False

    async def send(self, *argv: str):
        assert argv
        self.stream.writer.write((shlex.join(argv) + "\n").encode())
        await self.stream.writer.drain()
        log.debug(f"Sent `{shlex.join(argv)}` to {self.id}")

    async def recv(self) -> list[str]:
        out = await self.stream.reader.readline()
        if not out or not self.alive:
            raise ConnectionAbortedError()
        argv = shlex.split(out.decode())
        assert argv
        log.debug(f"Received `{shlex.join(argv)}` from {self.id}")
        return argv

    async def try_send(self, *argv: str):
        assert argv

        try:
            await self.send(*argv)

        except ConnectionError:
            log.warning(f"Could not send `{argv[0]}`, connection was lost.")

        except Exception as e:
            log.warning(f"Could not send `{argv[0]}`, unexpected error occurred.", e)
