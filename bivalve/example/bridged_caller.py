# --------------------------------------------------------------------
# bridged_caller.py
#
# Author: Lain Musgrove (lain.proliant@gmail.com)
# Date: Tuesday May 9, 2023
#
# Distributed under terms of the MIT license.
# --------------------------------------------------------------------

import asyncio
from bivalve.logging import LogManager

from .client import ExampleClient
from .server import ExampleServer


# --------------------------------------------------------------------
async def do_call_then_shutdown(conn: Connection, server: ExampleServer):
    pass

# --------------------------------------------------------------------
async def main():
    LogManager().setup()
    server = ExampleServer()
    conn = server.bridge()

    result = await conn.call("get_message")
    print(*result)

    await asyncio.gather(server.run(), do_call_then_shutdown(conn, server))


# --------------------------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
