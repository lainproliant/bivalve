# --------------------------------------------------------------------
# call_client.py
#
# Author: Lain Musgrove (lain.musgrove@hearst.com)
# Date: Saturday August 12, 2023
# --------------------------------------------------------------------

import asyncio
import shlex
import signal
import threading
import logging

from bivalve.agent import BivalveAgent
from bivalve.logging import LogManager
from bivalve.nio import NonBlockingTextInput
from bivalve.call import Response

# --------------------------------------------------------------------
LogManager().set_level(logging.DEBUG)
log = LogManager().get(__name__)

# --------------------------------------------------------------------
class ExampleClient(BivalveAgent):
    def __init__(self, host="", port=0):
        super().__init__()
        self.host = host
        self.port = port
        self.server_id = -1

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

    def on_connect(self, conn):
        self.server_id = conn.id

    def on_disconnect(self, _):
        self.shutdown()

    async def cmd_echo(self, _, msg: str):
        print(f"<< ECHO >> {msg}")

    async def _call_add_fn(self, *argv: str):
        future: asyncio.Future[Response] = asyncio.Future()
        await self.call(self.server_id, "add", [*argv], future)
        response = await future.result()
        print(f"<< RESPONSE >> {response}")

    def input_thread(self, loop):
        with NonBlockingTextInput() as ninput:
            while not self._shutdown_event.is_set():
                s = ninput.read("> ", timeout=1)
                if s is not None:
                    argv = shlex.split(s)
                    asyncio.run_coroutine_threadsafe(self._call_add_fn(*argv), loop)


# --------------------------------------------------------------------
def main():
    LogManager().setup()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    client = ExampleClient("localhost", 9595)
    loop.run_until_complete(client.run())

# --------------------------------------------------------------------
if __name__ == '__main__':
    main()
