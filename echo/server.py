# --------------------------------------------------------------------
# server.py
#
# Author: Lain Musgrove (lain.proliant@gmail.com)
# Date: Saturday February 11, 2023
#
# Distributed under terms of the MIT license.
# --------------------------------------------------------------------

import asyncio
from uuid import UUID

from .util import AsyncStream, Connection

# --------------------------------------------------------------------
class AsyncEchoServer:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.running = True
        self.connections: dict[UUID, Connection] = {}

    async def start(self):
        await AsyncStream.start_server(self.on_client_connect, self.host, self.port)
        print(f"Server started on {self.host}:{self.port}")

    def print_total_clients(self):
        print(f"{len(self.connections)} clients connected.")

    def on_client_connect(self, stream: AsyncStream):
        self.connections[stream.id] = Connection(
            stream, asyncio.create_task(self.communicate(stream))
        )
        print(f"Client connected: {stream.id}")
        self.print_total_clients()

    async def disconnect_client(self, stream: AsyncStream):
        await self.connections[stream.id].close()
        del self.connections[stream.id]
        print(f"Client disconnected: {stream.id}")
        self.print_total_clients()

    async def shutdown(self):
        for connection in self.connections.values():
            await connection.close()
        self.connections.clear()
        self.running = False

    async def communicate(self, stream: AsyncStream):
        while True:
            try:
                print('<< waiting to read line from client >>')
                out = await stream.reader.readline()
                if not out:
                    await self.disconnect_client(stream)
                    return

                print('<< line read from client >>')
                if out.decode().strip() == "quit":
                    await self.disconnect_client(stream)
                    return

                elif out.decode().strip() == "die":
                    await self.shutdown()
                    return

                else:
                    stream.writer.write(out)
                    print('<< response written to writer >>')
                    await stream.writer.drain()
                    print('<< writer drained >>')

            except Exception as e:
                print(f"Connection error: {e}")
                await self.disconnect_client(stream)
                return


# --------------------------------------------------------------------
async def main():
    server = AsyncEchoServer("localhost", 9595)
    await server.start()
    while server.running:
        await asyncio.sleep(1)

# --------------------------------------------------------------------
if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())
