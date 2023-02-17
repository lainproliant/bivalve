# --------------------------------------------------------------------
# server.py
#
# Author: Lain Musgrove (lain.proliant@gmail.com)
# Date: Friday February 17, 2023
#
# Distributed under terms of the MIT license.
# --------------------------------------------------------------------

import asyncio
import signal

from bivalve.aio import Connection
from bivalve.agent import BivalveAgent
from bivalve.logging import LogManager

# --------------------------------------------------------------------
log = LogManager().get(__name__)

# --------------------------------------------------------------------
class ExampleServer(BivalveAgent):
    def __init__(self, host: str, port: int):
        super().__init__()
        self.host = host
        self.port = port

    def ctrlc_handler(self, *_):
        log.critical("Ctrl+C received.")
        self.shutdown_event.set()

    async def run(self):
        signal.signal(signal.SIGINT, self.ctrlc_handler)
        await self.serve(self.host, self.port)
        await super().run()

    async def cmd_echo(self, conn: Connection, msg: str):
        await conn.send("echo", msg)

    async def cmd_quit(self, conn: Connection):
        await self.disconnect(conn)


# --------------------------------------------------------------------
def main():
    LogManager().setup()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    server = ExampleServer("localhost", 9595)
    loop.run_until_complete(server.run())

# --------------------------------------------------------------------
if __name__ == '__main__':
    main()
