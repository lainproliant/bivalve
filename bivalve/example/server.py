# --------------------------------------------------------------------
# server.py
#
# Author: Lain Musgrove (lain.proliant@gmail.com)
# Date: Friday February 17, 2023
#
# Distributed under terms of the MIT license.
# --------------------------------------------------------------------

import asyncio
import logging
import signal
import sys
import tracemalloc

from bivalve.agent import BivalveAgent
from bivalve.aio import Connection
from bivalve.logging import LogManager

# --------------------------------------------------------------------
tracemalloc.start()
log = LogManager().get(__name__)


# --------------------------------------------------------------------
class ExampleServer(BivalveAgent):
    def __init__(self, host="", port=0):
        super().__init__()
        self.host = host
        self.port = port

    def ctrlc_handler(self, *_):
        log.critical("Ctrl+C received.")
        self.shutdown()

    async def run(self):
        signal.signal(signal.SIGINT, self.ctrlc_handler)
        if self.host and self.port:
            await self.serve(host=self.host, port=self.port)
        await super().run()

    def on_connect(self, conn: Connection):
        self.schedule(conn.send("echo", "Hello, there!"))

    def fn_add(self, conn: Connection, *argv):
        return sum([int(s) for s in argv])

    async def cmd_echo(self, conn: Connection, msg: str):
        await conn.send("echo", msg)

    async def cmd_quit(self, conn: Connection):
        await self.disconnect(conn)

    def fn_add(self, conn: Connection, *argv):
        total = 0
        for arg in argv:
            total += int(arg)
        return total


# --------------------------------------------------------------------
def main():
    LogManager().setup()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    if "--debug" in sys.argv:
        LogManager().set_level(logging.DEBUG)

    server = ExampleServer("localhost", 9595)
    loop.run_until_complete(server.run())


# --------------------------------------------------------------------
if __name__ == "__main__":
    main()
