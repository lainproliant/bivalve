# --------------------------------------------------------------------
# unix_server.py
#
# Author: Lain Musgrove (lain.proliant@gmail.com)
# Date: Thursday July 6, 2023
#
# Distributed under terms of the MIT license.
# --------------------------------------------------------------------

import asyncio
import logging
import signal
import sys

from bivalve.agent import BivalveAgent
from bivalve.aio import Connection
from bivalve.logging import LogManager

# --------------------------------------------------------------------
log = LogManager().get(__name__)


# --------------------------------------------------------------------
class ExamplePathServer(BivalveAgent):
    def __init__(self, path):
        super().__init__()
        self.path = path

    def ctrlc_handler(self, *_):
        log.critical("Ctrl+C received.")
        self._shutdown_event.set()

    async def run(self):
        signal.signal(signal.SIGINT, self.ctrlc_handler)
        if self.path:
            await self.serve(path=self.path)
        await super().run()

    def on_connect(self, conn: Connection):
        self.schedule(conn.send("echo", "Hello, there!"))

    async def cmd_echo(self, conn: Connection, msg: str):
        await conn.send("echo", msg)

    async def cmd_quit(self, conn: Connection):
        await self.disconnect(conn)


# --------------------------------------------------------------------
def main():
    LogManager().setup()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    if "--debug" in sys.argv:
        LogManager().set_level(logging.DEBUG)

    server = ExamplePathServer("./unix.sock")
    loop.run_until_complete(server.run())


# --------------------------------------------------------------------
if __name__ == "__main__":
    main()
