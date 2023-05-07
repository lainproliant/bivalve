# --------------------------------------------------------------------
# bridge.py
#
# Author: Lain Musgrove (lain.proliant@gmail.com)
# Date: Saturday May 6, 2023
#
# Distributed under terms of the MIT license.
# --------------------------------------------------------------------

import asyncio
from bivalve.logging import LogManager

from .client import ExampleClient
from .server import ExampleServer


# --------------------------------------------------------------------
async def main():
    LogManager().setup()
    server = ExampleServer()
    client = ExampleClient()
    server.add_connection(client.bridge())

    await asyncio.gather(server.run(), client.run())


# --------------------------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
