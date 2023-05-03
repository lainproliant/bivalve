# --------------------------------------------------------------------
# agent.py
#
# Author: Lain Musgrove (lain.proliant@gmail.com)
# Date: Thursday February 16, 2023
#
# Distributed under terms of the MIT license.
# --------------------------------------------------------------------

import asyncio
from datetime import datetime, timedelta

from bivalve.aio import Connection, Stream
from bivalve.logging import LogManager
from bivalve.util import Commands

log = LogManager().get(__name__)

# --------------------------------------------------------------------
class BivalveAgent:
    def __init__(self):
        self.servers: list[asyncio.Server] = []
        self.commands = Commands(self)
        self.connections: dict[Connection.ID, Connection] = {}
        self.shutdown_event = asyncio.Event()
        self.syn_schedule = timedelta(seconds=5)
        self.syn_timeout = timedelta(seconds=5)
        self.max_conns = 0  # no maximum connections

    @property
    def running(self) -> bool:
        return bool(self.connections or self.servers)

    async def serve(self, host: str, port: int, ssl=None):
        server = await Stream.start_server(self.on_client_connect, host, port, ssl)
        self.servers.append(server)
        log.info(f"Serving peers on {host}:{port} (ssl={ssl}).")

    async def connect(self, host: str, port: int, ssl=None):
        stream = await Stream.connect(host, port, ssl)
        conn = Connection(stream, asyncio.create_task(self.communicate(stream)))
        log.info("Connected to peer on {host}:{port} (ssl={ssl}).")
        self.connections[conn.id] = conn

    async def on_client_connect(self, stream: Stream):
        if self.max_conns and len(self.connections) >= self.max_conns:
            await stream.close()
            log.warning(
                f"Maximum number of connections reached ({self.max_conns}), rejected connection."
            )
            return

        self.connections[stream.id] = Connection(
            stream, asyncio.create_task(self.communicate(stream))
        )
        log.info(f"Client connected: {stream.id}")

    async def on_client_disconnect(self, stream: Stream):
        pass

    async def disconnect(self, conn: Connection, notify=True):
        if notify:
            await conn.try_send("bye")
        await conn.close()
        if conn.id in self.connections:
            del self.connections[conn.id]
            log.info(f"Peer disconnected: {conn.id}.")
        await self.on_client_disconnect(conn.stream)

    async def maintain(self):
        now = datetime.now()
        trash: list[Connection] = []

        for conn in self.connections.values():
            try:
                if conn.ack_ttl and conn.ack_ttl <= now:
                    log.warning("Peer syn timed out: {conn.id}")
                    trash.append(conn)
                elif conn.syn_at <= now:
                    await conn.send("syn")
                    conn.syn_at = datetime.max
                    conn.ack_ttl = datetime.now() + self.syn_timeout

            except Exception as e:
                log.error(f"Error managing connection for peer {conn.id}.", e)

        for conn in trash:
            await self.disconnect(conn, notify=False)

        if self.shutdown_event.is_set():
            await self._shutdown()

    async def process_command(self, conn: Connection, argv: list[str]):
        try:
            command = self.commands.get(argv[0])
            if command is None:
                raise ValueError(f"Command is not recognized: {argv[0]}.")
            await command(conn, *argv[1:])

        except ConnectionError:
            raise

        except Exception as e:
            log.error("Error processing peer command.", e)

    async def communicate(self, stream: Stream):
        conn = self.connections[stream.id]
        while conn.alive:
            try:
                argv = await conn.recv()
                await self.process_command(conn, argv)

            except ConnectionError:
                log.warning(f"Connection lost with peer {conn.id}.")
                await self.disconnect(conn, notify=False)

    async def run(self):
        while self.running:
            await self.maintain()
            await asyncio.sleep(0)

    def shutdown(self):
        self.shutdown_event.set()

    async def _shutdown(self):
        if not self.running:
            return

        log.info("Shutting down.")
        try:
            for conn in list(self.connections.values()):
                await self.disconnect(conn)
            for server in self.servers:
                server.close()

        except Exception as e:
            log.error("Error while shutting down.", e)

        finally:
            self.connections.clear()
            self.servers.clear()
            log.info("Shutdown complete.")

    async def send(self, *argv):
        for conn in self.connections.values():
            await conn.send(*argv)

    async def cmd_syn(self, conn: Connection):
        await conn.send("ack")

    async def cmd_ack(self, conn: Connection):
        conn.ack_ttl = None
        conn.syn_at = datetime.now() + self.syn_schedule

    async def cmd_bye(self, conn: Connection):
        await self.disconnect(conn, notify=False)
