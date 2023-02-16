# --------------------------------------------------------------------
# client.py
#
# Author: Lain Musgrove (lain.proliant@gmail.com)
# Date: Saturday February 11, 2023
#
# Distributed under terms of the MIT license.
# --------------------------------------------------------------------

import asyncio
import threading
from typing import Optional

from .util import AsyncStream


# --------------------------------------------------------------------
class AsyncEchoClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.running = True
        self.stream: Optional[AsyncStream] = None
        self.input_thread: Optional[threading.Thread] = None

    async def start(self):
        self.stream = await AsyncStream.connect(self.host, self.port)
        self.input_thread = threading.Thread(target=self._input_thread)
        self.input_thread.start()

    async def listener_thread(self):
        assert self.stream

        while self.running:
            out = await self.stream.reader.readline()
            if out.decode().strip() == "bye":
                self.running = False
                await self.stream.close()

            else:
                print(out.decode().strip())

    def _input_thread(self):
        loop = asyncio.new_event_loop()
        loop.run_until_complete(self.handle_input())

    async def handle_input(self):
        assert self.stream

        while self.running:
            s = input("echo> ")
            self.stream.writer.writelines([s.encode()])
            await self.stream.writer.drain()


# --------------------------------------------------------------------
async def main():
    client = AsyncEchoClient("localhost", 9595)
    await client.start()
    while client.running:
        await asyncio.sleep(1)


# --------------------------------------------------------------------
if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())
