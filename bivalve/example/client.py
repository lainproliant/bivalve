# --------------------------------------------------------------------
# client.py
#
# Author: Lain Musgrove (lain.proliant@gmail.com)
# Date: Friday February 17, 2023
#
# Distributed under terms of the MIT license.
# --------------------------------------------------------------------

import asyncio
import shlex
import signal
import threading

from bivalve.agent import BivalveAgent
from bivalve.logging import LogManager
from bivalve.nio import NonBlockingTextInput

# --------------------------------------------------------------------
log = LogManager().get(__name__)

# --------------------------------------------------------------------
class ExampleClient(BivalveAgent):
    def __init__(self, host: str, port: int):
        super().__init__()
        self.host = host
        self.port = port

    def ctrlc_handler(self, *_):
        log.critical("Ctrl+C received.")
        self.shutdown()

    async def run(self):
        signal.signal(signal.SIGINT, self.ctrlc_handler)
        try:
            await self.connect(self.host, self.port)
        except Exception as e:
            log.fatal("Failed to connect to server.", e)
            self.shutdown()

        loop = asyncio.get_event_loop()
        thread = threading.Thread(target=self.input_thread, args=(loop,))
        thread.start()
        await super().run()
        thread.join()

    async def on_client_disconnect(self, _):
        self.shutdown()

    async def cmd_echo(self, _, msg: str):
        print(f"<< ECHO >> {msg}")

    def input_thread(self, loop):
        ninput = NonBlockingTextInput()
        ninput.start()

        while not self.shutdown_event.is_set():
            s = ninput.read("> ", timeout=1)
            if s is not None:
                argv = shlex.split(s)
                asyncio.run_coroutine_threadsafe(self.send("echo", *argv), loop)

        ninput.stop()


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
