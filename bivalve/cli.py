#!/usr/bin/env python
# --------------------------------------------------------------------
# cli.py
#
# Author: Lain Musgrove (lain.musgrove@hearst.com)
# Date: Monday October 9, 2023
# --------------------------------------------------------------------

import asyncio
import getpass
import shlex
import signal
import ssl
import threading
import time
import traceback
from argparse import ArgumentParser
from dataclasses import dataclass
from typing import Optional

from bivalve.agent import BivalveAgent
from bivalve.call import Call
from bivalve.logging import LogManager
from bivalve.nio import NonBlockingTextInput

# --------------------------------------------------------------------
log = LogManager().get(__name__)


# --------------------------------------------------------------------
@dataclass
class Config:
    host: Optional[str] = None
    port: Optional[int] = None
    sock: Optional[str] = None
    debug = False
    ssl = False
    verify_cert = True
    check_hostname = True

    def _argparser(self) -> ArgumentParser:
        parser = ArgumentParser(description="bivalve agent client")

        parser.add_argument(
            "--host",
            "-H",
            type=str,
            help="Host to connect to.  Requires --port/-p to be specified.",
        )
        parser.add_argument(
            "--port",
            "-p",
            type=int,
            help="Port on the host to connect to.  Requires --host/-h to be specified.",
        )
        parser.add_argument(
            "--sock", "-s", type=str, help="Path to a UNIX socket file to connect to."
        )
        parser.add_argument(
            "--ssl",
            "-S",
            action="store_true",
            help="Connect to an SSL encrypted socket.",
        )
        parser.add_argument(
            "--debug", action="store_true", help="Print DEBUG level logs."
        )
        parser.add_argument(
            "--no-check-hostname",
            dest="check_hostname",
            action="store_false",
            help="Disable SSL cert hostname check.",
        )
        parser.add_argument(
            "--no-verify-cert",
            dest="verify_cert",
            action="store_false",
            help="Disable SSL cert verification.",
        )

        return parser

    def parse_args(self):
        self._argparser().parse_args(namespace=self)
        if (self.host is None or self.port is None) and (self.sock is None):
            raise ValueError("One of host/port or sock must be specified.")
        return self


# --------------------------------------------------------------------
class ClientAgent(BivalveAgent):
    def __init__(self, config: Config):
        super().__init__()
        self.config = config

    def ctrlc_handler(self, *_):
        log.critical("Ctrl+C received.")
        self.shutdown()

    async def run(self):
        signal.signal(signal.SIGINT, self.ctrlc_handler)

        ssl_ctx: Optional[ssl.SSLContext] = None

        if self.config.ssl:
            ssl_ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            ssl_ctx.check_hostname = self.config.check_hostname
            ssl_ctx.verify_mode = ssl.CERT_NONE
            if self.config.verify_cert:
                ssl_ctx.verify_mode = ssl.CERT_REQUIRED

        await self.connect(
            host=self.config.host,
            port=self.config.port,
            path=self.config.sock,
            ssl=ssl_ctx,
        )

        loop = asyncio.get_event_loop()
        thread = threading.Thread(target=self.input_thread, args=(loop,))
        thread.start()
        await super().run()
        thread.join()

    def on_disconnect(self, _):
        self.shutdown()

    def on_unrecognized_command(self, conn, *argv):
        print(f"<< CMD {' '.join(argv)}")

    def input_thread(self, loop):
        current_call: Optional[Call] = None

        asyncio.set_event_loop(loop)

        with NonBlockingTextInput() as ninput:
            while not self._shutdown_event.is_set():
                if current_call:
                    try:
                        result = current_call.future.result()

                        if result.code == result.Code.OK:
                            print(f"<< OK {' '.join(result.content)}")

                        elif result.code == result.Code.ERROR:
                            print(f"<< ERROR {' '.join(result.content)}")

                        current_call = None

                    except asyncio.InvalidStateError:
                        time.sleep(0.25)

                    except Exception:
                        traceback.print_exc()
                        current_call = None
                    continue

                s = ninput.read("> ", timeout=0.25)
                if s is not None:
                    argv_in = shlex.split(s)
                    if not argv_in:
                        continue

                    argv = []

                    while argv_in:
                        arg = argv_in.pop(0)
                        if arg == "<>":
                            argv.append(getpass.getpass("pwd > "))
                        else:
                            argv.append(arg)

                    if argv[0] == "call":
                        current_call = self.call(*argv[1:])

                    else:
                        asyncio.run_coroutine_threadsafe(
                            self.send_to(self.peers(), *argv), loop
                        )


# --------------------------------------------------------------------
def main():
    LogManager().setup()
    config = Config().parse_args()

    if config.debug:
        LogManager().set_level("DEBUG")
    else:
        LogManager().set_level("WARNING")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    client = ClientAgent(config)
    loop.run_until_complete(client.run())
    print()


# --------------------------------------------------------------------
if __name__ == "__main__":
    main()
