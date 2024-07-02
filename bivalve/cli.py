#!/usr/bin/env python
# --------------------------------------------------------------------
# cli.py
#
# Author: Lain Musgrove (lain.musgrove@hearst.com)
# Date: Monday October 9, 2023
# --------------------------------------------------------------------

import argparse
import asyncio
import getpass
import os
import shlex
import signal
import ssl
import sys
import threading
import time
import traceback
from dataclasses import dataclass
from typing import Optional

import waterlog

from bivalve.agent import BivalveAgent
from bivalve.call import Call
from bivalve.nio import NonBlockingTextInput

# --------------------------------------------------------------------
log = waterlog.get(__name__)


# --------------------------------------------------------------------
@dataclass
class Config:
    host: Optional[str] = None
    port: Optional[int] = None
    sock: Optional[str] = None
    script = ""
    debug = False
    ssl = False
    verify_cert = True
    check_hostname = True
    args: Optional[list[str]] = None

    def _argparser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description="bivalve agent client")

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
        parser.add_argument(
            "--script",
            "-x",
            help="Execute semicolon-delimited commands from the given file, then exit.",
        )
        parser.add_argument(
            "args", nargs=argparse.REMAINDER, help="A command to execute."
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
        self.tasks: list[asyncio.Task] = []

    def ctrlc_handler(self, *_):
        log.critical("Ctrl+C received.")
        self.shutdown()

    def _load_script(self, filename: str) -> list[str]:
        if filename == "-":
            script = sys.stdin.read()
        else:
            with open(filename, "r") as infile:
                script = infile.read()

        commands = script.split("\n")
        return commands

    async def run(self):
        signal.signal(signal.SIGINT, self.ctrlc_handler)
        loop = asyncio.get_event_loop()

        if self.config.args:
            commands = []
            if self.config.script:
                commands.extend(self._load_script(self.config.script))
            commands.append(shlex.join(self.config.args))
            self.tasks.append(asyncio.create_task(self.script_task(commands)))

        elif self.config.script:
            commands = self._load_script(self.config.script)
            self.tasks.append(asyncio.create_task(self.script_task(commands)))

        else:
            self.tasks.append(asyncio.create_task(self.repl_task()))

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

        await super().run()

    def on_disconnect(self, _):
        self.shutdown()

    def on_unrecognized_command(self, conn, *argv):
        print(f"<< CMD {' '.join(argv)}")

    async def script_task(self, commands):
        current_call: Optional[Call] = None
        quiet = False

        try:
            while not self._shutdown_event.is_set() and (commands or current_call):
                await asyncio.sleep(0)

                if current_call:
                    try:
                        result = current_call.future.result()
                        if not quiet and result.code == result.Code.OK:
                            sys.stdout.write(" ".join(result.content))
                            sys.stdout.flush()

                        elif result.code == result.Code.ERROR:
                            print(f"ERROR: {' '.join(result.content)}", file=sys.stderr)
                            break

                        current_call = None
                        quiet = False

                    except asyncio.InvalidStateError:
                        pass

                    except Exception:
                        traceback.print_exc()
                        current_call = None
                        quiet = False

                    continue

                command: str = commands.pop(0)
                command = command.format(**os.environ)

                argv = shlex.split(command)
                if not argv:
                    continue
                if argv[0] == "call":
                    current_call = self.call(*argv[1:], timeout_ms=0)
                elif argv[0] == "@call":
                    current_call = self.call(*argv[1:], timeout_ms=0)
                    quiet = True
                else:
                    self.schedule(self.send_to(self.peers(), *argv))

        finally:
            self.shutdown()

    async def repl_task(self):
        current_call: Optional[Call] = None

        with NonBlockingTextInput() as ninput:
            while not self._shutdown_event.is_set():
                await asyncio.sleep(0)

                if current_call:
                    try:
                        result = current_call.future.result()

                        if result.code == result.Code.OK:
                            print()
                            print(f"<< OK {' '.join(result.content)}")

                        elif result.code == result.Code.ERROR:
                            print()
                            print(f"<< ERROR {' '.join(result.content)}")

                        current_call = None

                    except asyncio.InvalidStateError:
                        sys.stdout.write(".")
                        sys.stdout.flush()
                        await asyncio.sleep(0.25)

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
                        current_call = self.call(*argv[1:], timeout_ms=0)

                    else:
                        self.schedule(self.send_to(self.peers(), *argv))

        # Print a newline before we exit repl.
        print()


# --------------------------------------------------------------------
def main():
    waterlog.setup()
    config = Config().parse_args()

    if config.debug:
        waterlog.set_level("DEBUG")
    else:
        waterlog.set_level("WARNING")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    client = ClientAgent(config)
    loop.run_until_complete(client.run())


# --------------------------------------------------------------------
if __name__ == "__main__":
    main()
