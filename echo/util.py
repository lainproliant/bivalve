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
import os
import sys
import selectors
from io import StringIO
from dataclasses import dataclass, field
from typing import Callable, Optional, TextIO
from uuid import UUID, uuid4


# --------------------------------------------------------------------
@dataclass
class NIOTextInput:
    def __init__(self, infile: TextIO = sys.stdin, promptfile: TextIO=sys.stdout):
        self.infile = infile
        self.promptfile = promptfile
        self.orig_fl: Optional[int] = None
        self.selector = selectors.DefaultSelector()
        self.timed_out = False

    def start(self):
        assert self.orig_fl is None
        self.orig_fl = fcntl.fcntl(self.infile, fcntl.F_GETFL)
        fcntl.fcntl(self.infile, fcntl.F_SETFL, self.orig_fl | os.O_NONBLOCK)
        self.selector.register(self.infile, selectors.EVENT_READ)

    def read(self, prompt: Optional[str] = None, timeout: Optional[int] = None):
        if prompt is not None and not self.timed_out:
            self.promptfile.write(prompt)
            self.promptfile.flush()
        result = self.selector.select(timeout=timeout)
        if result:
            self.timed_out = False
            return self.infile.read()
        else:
            self.timed_out = True
            return None

    def stop(self):
        assert self.orig_fl
        fcntl.fcntl(self.infile, fcntl.F_SETFL, self.orig_fl)
        self.orig_fl = None


# --------------------------------------------------------------------
@dataclass
class AsyncStream:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    id: UUID = field(default_factory=uuid4)

    async def close(self):
        self.writer.close()
        await self.writer.wait_closed()

    @staticmethod
    async def connect(host: str, port: int):
        reader, writer = await asyncio.open_connection(host, port)
        return AsyncStream(reader, writer)

    @staticmethod
    async def start_server(
        callback: Callable[["AsyncStream"], None], host: str, port: int
    ):
        def connected_callback(reader, writer):
            callback(AsyncStream(reader, writer))

        await asyncio.start_server(connected_callback, host, port)


# --------------------------------------------------------------------
@dataclass
class Connection:
    stream: AsyncStream
    task: asyncio.Task

    async def close(self):
        await self.stream.close()
