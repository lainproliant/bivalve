# --------------------------------------------------------------------
# client.py
#
# Author: Lain Musgrove (lain.proliant@gmail.com)
# Date: Friday February 17, 2023
#
# Distributed under terms of the MIT license.
# --------------------------------------------------------------------

import asyncio
import logging
import shlex
import signal
import sys
import threading

from bivalve.agent import BivalveAgent
from bivalve.aio import Connection
from bivalve.logging import LogManager
from bivalve.nio import NonBlockingTextInput

# --------------------------------------------------------------------
log = LogManager().get(__name__)


# --------------------------------------------------------------------
class ExampleClient(BivalveAgent):
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
            try:
                await self.connect(host=self.host, port=self.port)
            except Exception:
                log.exception("Failed to connect to server.")
                self.shutdown()

        loop = asyncio.get_event_loop()
        thread = threading.Thread(target=self.input_thread, args=(loop,))
        thread.start()
        await super().run()
        thread.join()

    def on_connect(self, conn: Connection):
        self.schedule(self.call_add(conn.id))

    async def call_add(self, conn_id):
        result = await self.call("add", 1, 2, 3)
        print("The result was:", result)

    def on_disconnect(self, _):
        self.shutdown()

    async def cmd_echo(self, _, msg: str):
        print(f"<< ECHO >> {msg}")

    def input_thread(self, loop):
        with NonBlockingTextInput() as ninput:
            while not self._shutdown_event.is_set():
                s = ninput.read("> ", timeout=1)
                if s is not None:
                    argv = shlex.split(s)
                    asyncio.run_coroutine_threadsafe(self.send_to(self.peers(), "echo", *argv), loop)


# --------------------------------------------------------------------
def main():
    LogManager().setup()

    if "--debug" in sys.argv:
        LogManager().set_level(logging.DEBUG)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    client = ExampleClient("localhost", 9595)
    loop.run_until_complete(client.run())


# --------------------------------------------------------------------
if __name__ == "__main__":
    main()
