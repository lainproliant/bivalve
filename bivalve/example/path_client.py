# --------------------------------------------------------------------
# path_client.py
#
# Author: Lain Musgrove (lain.proliant@gmail.com)
# Date: Thursday July 6, 2023
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
from bivalve.logging import LogManager
from bivalve.nio import NonBlockingTextInput

# --------------------------------------------------------------------
log = LogManager().get(__name__)


# --------------------------------------------------------------------
class ExamplePathClient(BivalveAgent):
    def __init__(self, path):
        super().__init__()
        self.path = path

    def ctrlc_handler(self, *_):
        log.critical("Ctrl+C received.")
        self.shutdown()

    async def run(self):
        signal.signal(signal.SIGINT, self.ctrlc_handler)
        try:
            await self.connect(path=self.path)
        except Exception as e:
            log.exception("Failed to connect to server.")
            self.shutdown()

        loop = asyncio.get_event_loop()
        thread = threading.Thread(target=self.input_thread, args=(loop,))
        thread.start()
        await super().run()
        thread.join()

    async def on_disconnect(self, _):
        self.shutdown()

    async def cmd_echo(self, _, msg: str):
        print(f"<< ECHO >> {msg}")

    def input_thread(self, loop):
        with NonBlockingTextInput() as ninput:
            while not self._shutdown_event.is_set():
                s = ninput.read("> ", timeout=1)
                if s is not None:
                    argv = shlex.split(s)
                    asyncio.run_coroutine_threadsafe(self.send("echo", *argv), loop)


# --------------------------------------------------------------------
def main():
    LogManager().setup()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    if "--debug" in sys.argv:
        LogManager().set_level(logging.DEBUG)

    client = ExamplePathClient("./unix.sock")
    loop.run_until_complete(client.run())


# --------------------------------------------------------------------
if __name__ == "__main__":
    main()
