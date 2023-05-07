# --------------------------------------------------------------------
# agent.py
#
# Author: Lain Musgrove (lain.proliant@gmail.com)
# Date: Thursday February 16, 2023
#
# Distributed under terms of the MIT license.
# --------------------------------------------------------------------

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Awaitable, Optional

from bivalve.aio import BridgeConnection, Connection, Stream, StreamConnection
from bivalve.logging import LogManager
from bivalve.util import Commands

log = LogManager().get(__name__)


# --------------------------------------------------------------------
@dataclass
class ConnectionContext:
    conn: Connection
    task: asyncio.Task
    ack_ttl: Optional[datetime] = None
    syn_at: datetime = datetime.min


# --------------------------------------------------------------------
class BivalveAgent:
    def __init__(
        self,
        max_peers=0,  # no maximum connections
        syn_schedule=timedelta(seconds=15),
        syn_timeout=timedelta(seconds=5),
    ):
        self._commands = Commands(self)
        self._conn_ctx_map: dict[Connection.ID, ConnectionContext] = {}
        self._max_peers = max_peers
        self._scheduled: list[Awaitable] = []
        self._servers: list[asyncio.Server] = []
        self._shutdown_event = asyncio.Event()
        self._syn_schedule = syn_schedule
        self._syn_timeout = syn_timeout

    @property
    def running(self) -> bool:
        return bool(self._conn_ctx_map or self._servers)

    async def serve(self, host: str, port: int, ssl=None):
        server = await Stream.start_server(self.on_incoming_stream, host, port, ssl)
        self._servers.append(server)
        log.info(f"Serving peers on {host}:{port} (ssl={ssl}).")

    async def connect(self, host: str, port: int, ssl=None) -> Connection:
        if self._max_peers and len(self._conn_ctx_map) >= self._max_peers:
            log.warning(
                f"Cancelled outbound connection: maximum number of peers reached ({self._max_peers})."
            )
            raise RuntimeError("Maximum number of peer connections reached.")

        stream = await Stream.connect(host, port, ssl)
        conn = StreamConnection(stream)
        log.info("Outbound peer connection on {host}:{port} (ssl={ssl}) established.")
        self.add_connection(conn)
        return conn

    def bridge(self) -> Connection:
        if self._max_peers and len(self._conn_ctx_map) >= self._max_peers:
            log.warning(
                f"Cancelled outbound connection: maximum number of peers reached ({self._max_peers})."
            )
            raise RuntimeError("Maximum number of peer connections reached.")

        send_queue: asyncio.Queue[str] = asyncio.Queue()
        recv_queue: asyncio.Queue[str] = asyncio.Queue()

        our_conn = BridgeConnection(send_queue, recv_queue)
        their_conn = BridgeConnection(recv_queue, send_queue)
        log.info(f"Bridge connection established: {our_conn.id}")
        self.add_connection(our_conn)
        return their_conn

    async def on_incoming_stream(self, stream: Stream):
        if self._max_peers and len(self._conn_ctx_map) >= self._max_peers:
            await stream.close()
            log.warning(
                f"Rejected incoming connection: maximum number of peers reached ({self._max_peers})."
            )
            return

        conn = StreamConnection(stream)
        log.info(f"Incoming peer connection established: {stream.id}")
        self.add_connection(conn)

    def add_connection(self, conn: Connection):
        self._conn_ctx_map[conn.id] = ConnectionContext(
            conn, asyncio.create_task(self.communicate(conn))
        )

        loop = asyncio.get_running_loop()
        loop.call_soon(self.on_connect, conn)

    def schedule(self, awaitable: Awaitable):
        self._scheduled.append(awaitable)

    def on_connect(self, conn: Connection):
        pass

    def on_disconnect(self, conn: Connection):
        pass

    def on_unrecognized_command(self, *argv):
        pass

    def disconnect(self, conn: Connection, notify=True):
        ctx = self._conn_ctx_map.get(conn.id)
        if ctx and ctx.task:
            ctx.task.cancel()
        ctx.task = asyncio.create_task(self._cleanup(conn, notify))

    async def _cleanup(self, conn: Connection, notify=True):
        if notify:
            await conn.try_send("bye")
        await conn.close()
        if conn.id in self._conn_ctx_map:
            del self._conn_ctx_map[conn.id]
            log.info(f"Peer disconnected: {conn.id}.")
            loop = asyncio.get_running_loop()
            loop.call_soon(self.on_disconnect, conn)

    async def maintain(self):
        trash: list[Connection] = []

        if self._scheduled:
            await asyncio.gather(*self._scheduled)
            self._scheduled.clear()

        now = datetime.now()

        for ctx in self._conn_ctx_map.values():
            try:
                if ctx.ack_ttl and ctx.ack_ttl <= now:
                    log.warning(f"Peer keepalive timed out: {ctx.conn.id}")
                    trash.append(ctx.conn)
                elif ctx.syn_at <= now:
                    await ctx.conn.send("syn")
                    ctx.syn_at = datetime.max
                    ctx.ack_ttl = datetime.now() + self._syn_timeout

            except Exception as e:
                log.error(f"Error managing connection for peer {ctx.conn.id}.", e)

        for conn in trash:
            await self._cleanup(conn, notify=False)

        if self._shutdown_event.is_set():
            await self._shutdown()

    async def process_command(self, conn: Connection, *argv: str):
        try:
            if len(argv) < 1:
                raise ValueError("No peer command was specified.")
            command = self._commands.get(argv[0])
            if command is None:
                raise ValueError(f"Peer command is not recognized: {argv[0]}.")
            await command(conn, *argv[1:])

        except ConnectionError:
            raise

        except Exception as e:
            log.error("Error processing peer command.", e)

    async def communicate(self, conn: Connection):
        while conn.alive:
            try:
                argv = await conn.recv()
                try:
                    await self.process_command(conn, *argv)
                except ValueError:
                    log.exception("Received an unrecognized peer command.")
                    loop = asyncio.get_running_loop()
                    loop.call_soon(self.on_unrecognized_command, conn, *argv)

            except ConnectionError:
                log.warning(f"Connection lost with peer {conn.id}.")
                await self._cleanup(conn, notify=False)

    async def run(self):
        while self.running:
            await self.maintain()
            await asyncio.sleep(0)

    def shutdown(self):
        self._shutdown_event.set()

    async def _shutdown(self):
        if not self.running:
            return

        log.info("Shutting down.")
        try:
            for ctx in list(self._conn_ctx_map.values()):
                await self._cleanup(ctx.conn)
            for server in self._servers:
                server.close()

        except Exception as e:
            log.error("Error while shutting down.", e)

        finally:
            self._conn_ctx_map.clear()
            self._servers.clear()
            log.info("Shutdown complete.")

    async def send(self, *argv):
        assert self._conn_ctx_map, "No connected peers."
        await asyncio.gather(
            *(ctx.conn.send(*argv) for ctx in self._conn_ctx_map.values())
        )

    async def cmd_syn(self, conn: Connection):
        await conn.send("ack")

    async def cmd_ack(self, conn: Connection):
        ctx = self._conn_ctx_map[conn.id]
        ctx.ack_ttl = None
        ctx.syn_at = datetime.now() + self._syn_schedule

    async def cmd_bye(self, conn: Connection):
        self.disconnect(conn, notify=False)
