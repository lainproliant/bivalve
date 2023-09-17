# --------------------------------------------------------------------
# bridge.py
#
# Author: Lain Musgrove (lain.proliant@gmail.com)
# Date: Saturday May 6, 2023
#
# Distributed under terms of the MIT license.
# --------------------------------------------------------------------

import asyncio
import logging
import sys

from bivalve.logging import LogManager

from .client import ExampleClient
from .server import ExampleServer


# --------------------------------------------------------------------
async def main():
    LogManager().setup()

    if "--debug" in sys.argv:
        LogManager().set_level(logging.DEBUG)

    server = ExampleServer()
    client = ExampleClient()
    client.add_connection(server.bridge())

    await asyncio.gather(server.run(), client.run())


# --------------------------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
