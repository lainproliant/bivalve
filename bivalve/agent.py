# --------------------------------------------------------------------
# agent.py
#
# Author: Lain Musgrove (lain.proliant@gmail.com)
# Date: Thursday February 16, 2023
#
# Distributed under terms of the MIT license.
# --------------------------------------------------------------------

import asyncio
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum, auto
from typing import Any, Awaitable, Iterable, Optional, Union

from bivalve.aio import Connection, Server, Stream, StreamConnection
from bivalve.call import Call, Response
from bivalve.datatypes import ArgV
from bivalve.logging import LogManager
from bivalve.util import Commands, async_wrap, get_millis, is_iterable

log = LogManager().get(__name__)


# --------------------------------------------------------------------
class Role(StrEnum):
    NONE = auto()
    PEER = auto()
    DOWNSTREAM = auto()
    UPSTREAM = auto()


# --------------------------------------------------------------------
@dataclass
class ConnectionContext:
    conn: Connection
    role: Role
    task: asyncio.Task
    ack_ttl: Optional[datetime] = None
    syn_at: datetime = datetime.min
    call_map: dict[int, Call] = field(default_factory=dict)


# --------------------------------------------------------------------
class BivalveAgent:
    def __init__(
        self,
        max_peers=0,  # no maximum connections
        syn_schedule=timedelta(seconds=10),
        syn_timeout=timedelta(seconds=5),
        syn_jitter=5,
        loop_duration_ms=150,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        incoming_role: Role = Role.DOWNSTREAM,
        outgoing_role: Role = Role.UPSTREAM,
    ):
        self._commands = Commands(self)
        self._conn_ctx_map: dict[int, ConnectionContext] = {}
        self._functions = Commands(self, prefix="fn_")
        self._loop = loop
        self._max_peers = max_peers
        self._servers: list[Server] = []
        self._shutdown_event = asyncio.Event()
        self._loop_duration_ms = loop_duration_ms
        self._syn_jitter = syn_jitter
        self._syn_schedule = syn_schedule
        self._syn_timeout = syn_timeout
        self._scheduled_tasks = set()
        self._incoming_role = incoming_role
        self._outgoing_role = outgoing_role

    @property
    def running(self) -> bool:
        return bool(self._conn_ctx_map or self._servers)

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            self._loop = asyncio.get_event_loop()
        return self._loop

    def connection_contexts(self):
        yield from self._conn_ctx_map.values()

    def peers(self) -> Iterable[Connection]:
        for ctx in self.connection_contexts():
            yield ctx.conn

    def upstream_peers(self) -> Iterable[Connection]:
        for ctx in self.connection_contexts():
            if ctx.role in (Role.PEER, Role.UPSTREAM):
                yield ctx.conn

    def downstream_peers(self) -> Iterable[Connection]:
        for ctx in self.conneciton_contexts():
            if ctx.role in (Role.PEER, Role.DOWNSTREAM):
                yield ctx.conn

    async def serve(self, **kwargs) -> Server:
        self._check_max_peers()

        server = await Server.serve(self.on_incoming_stream, **kwargs)
        log.info(f"Serving peers on {server}.")
        self._servers.append(server)
        return server

    async def connect(self, role=Role.NONE, **kwargs) -> Connection:
        self._check_max_peers()

        if role == Role.NONE:
            role = self._outgoing_role

        stream = await Stream.connect(**kwargs)
        conn = StreamConnection(stream)
        self.add_connection(conn, role)

        return conn

    def _check_max_peers(self):
        if self._max_peers and len(self._conn_ctx_map) >= self._max_peers:
            log.warning(
                f"Cancelled peer connection: maximum number of peers reached ({self._max_peers})."
            )
            raise RuntimeError("Maximum number of peer connections reached.")

    def bridge(self, role=Role.NONE) -> Connection:
        self._check_max_peers()

        send_queue: asyncio.Queue[str] = asyncio.Queue()
        recv_queue: asyncio.Queue[str] = asyncio.Queue()

        if role == Role.NONE:
            role = self._incoming_role

        our_conn = Connection.bridge(send_queue, recv_queue)
        their_conn = Connection.bridge(recv_queue, send_queue)
        log.info(f"Bridge connected on {our_conn}.")
        self.add_connection(our_conn, role)
        return their_conn

    async def on_incoming_stream(self, stream: Stream):
        if self._max_peers and len(self._conn_ctx_map) >= self._max_peers:
            await stream.close()
            log.warning(
                f"Rejected incoming connection on {stream}: maximum number of peers reached ({self._max_peers})."
            )
            return

        conn = StreamConnection(stream)
        log.info(f"Incoming peer connected: {stream}")
        self.add_connection(conn, self._incoming_role)

    def add_connection(self, conn: Connection, role=Role.UPSTREAM):
        self._conn_ctx_map[conn.id] = ConnectionContext(
            conn, role, asyncio.create_task(self.communicate(conn))
        )
        self.schedule(self._on_connect(conn))

    def schedule(self, awaitable: Awaitable) -> asyncio.Task:
        """
        Schedule a task to be run in parallel.
        """
        task = self.loop.create_task(awaitable)
        self._scheduled_tasks.add(task)
        task.add_done_callback(self._scheduled_tasks.discard)
        return task

    async def _on_connect(self, conn: Connection):
        try:
            await async_wrap(self.on_connect, conn)
        except Exception:
            log.exception("Error occurred during `on_connect()` handler.")

    def on_connect(self, conn: Connection):
        pass

    async def _on_disconnect(self, conn: Connection):
        try:
            await async_wrap(self.on_disconnect, conn)
        except Exception:
            log.exception("Error occurred during `on_disconnect()` handler.")

    def on_disconnect(self, conn: Connection):
        pass

    async def _on_unrecognized_command(self, conn: Connection, *argv):
        try:
            await async_wrap(self.on_unrecognized_command, conn, *argv)
        except Exception:
            log.exception("Error occurred during `on_unrecognized_command()` handler.")

    def on_unrecognized_command(self, conn: Connection, *argv):
        pass

    async def _on_startup(self):
        try:
            await async_wrap(self.on_startup)
        except Exception:
            log.exception("Error occurred during `on_startup()` handler.")

    def on_startup(self):
        pass

    async def _on_shutdown(self):
        try:
            await async_wrap(self.on_shutdown)
        except Exception:
            log.exception("Error occurred during `on_shutdown()` handler.")

    def on_shutdown(self):
        pass

    def disconnect(self, conn: Connection, notify=True):
        ctx = self._conn_ctx_map.get(conn.id)
        if ctx and ctx.task:
            ctx.task.cancel()
        if ctx:
            ctx.task = asyncio.create_task(self._cleanup(conn, notify))

    async def _cleanup(self, conn: Connection, notify=True):
        if notify:
            await conn.try_send("bye")

        await conn.close()

        if conn.id in self._conn_ctx_map:
            del self._conn_ctx_map[conn.id]
            log.info(f"Peer disconnected: {conn}")
            self.schedule(self._on_disconnect(conn))

    async def maintain(self):
        trash: list[Connection] = []

        now = datetime.now()

        for ctx in self._conn_ctx_map.values():
            try:
                if ctx.ack_ttl and ctx.ack_ttl <= now:
                    log.warning(f"Peer keepalive timed out: {ctx.conn}")
                    trash.append(ctx.conn)
                elif ctx.syn_at <= now:
                    await ctx.conn.send("syn")
                    ctx.syn_at = datetime.max
                    ctx.ack_ttl = now + self._syn_timeout
                    self._cleanup_calls(now, ctx)

            except Exception:
                trash.append(ctx.conn)
                log.exception(
                    f"Error managing connection for peer, closing connection: {ctx.conn}"
                )

        for conn in trash:
            await self._cleanup(conn, notify=False)

        if self._shutdown_event.is_set():
            await self._shutdown()

    def _cleanup_calls(self, now: datetime, ctx: ConnectionContext):
        trash: list[int] = []

        for call in ctx.call_map.values():
            if now >= call.expires_at:
                if not call.future.done():
                    call.future.set_exception(TimeoutError())
                trash.append(call.id)

        for call_id in trash:
            del ctx.call_map[call_id]

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

        except Exception:
            log.exception("Error processing peer command.")

    async def communicate(self, conn: Connection):
        while await conn.alive():
            try:
                argv = await conn.recv()
                try:
                    await self.process_command(conn, *argv)
                except ValueError:
                    log.exception("Received an unrecognized peer command.")
                    self.schedule(self._on_unrecognized_command(conn, *argv))
            except ConnectionAbortedError:
                await self._cleanup(conn, notify=False)
            except ConnectionError:
                if await conn.alive():
                    log.exception(f"Connection lost with peer {conn.id}.")
                await self._cleanup(conn, notify=False)

    async def run(self):
        if self._loop is None:
            self._loop = asyncio.get_event_loop()

        self.schedule(self._on_startup())

        while self.running:
            now_ms = get_millis()
            await self.maintain()
            sleep_timeout = max(0, self._loop_duration_ms - (get_millis() - now_ms))
            await asyncio.sleep(sleep_timeout / 1000)

    def shutdown(self):
        self._shutdown_event.set()

    async def _shutdown(self):
        if not self.running:
            return

        log.info("Shutting down.")

        await self._on_shutdown()

        try:
            for ctx in list(self._conn_ctx_map.values()):
                await self._cleanup(ctx.conn)
            for server in self._servers:
                server.close()

        except Exception:
            log.exception("Error while shutting down.")

        finally:
            self._conn_ctx_map.clear()
            self._servers.clear()
            log.info("Shutdown complete.")

    async def send(self, *argv):
        await self.send_to(self.downstream_peers(), *argv)

    async def send_to(self, peer: Union[Connection, Iterable[Connection]], *argv):
        if is_iterable(peer):
            await asyncio.gather(*(conn.send(*argv) for conn in peer))
        else:
            await peer.send(*argv)

    def call(
        self,
        *params: Any,
        timeout_ms: int = 10000,
    ) -> Call:
        try:
            conn_id = random.choice([*self.upstream_peers()]).id
        except IndexError:
            raise RuntimeError("No upstream peers to call.")

        ctx = self._conn_ctx_map.get(conn_id)
        if ctx is None:
            raise ValueError("Not a connected peer: id={conn.id}.")

        fn, *argv = [str(x) for x in params]
        call = Call(fn, argv)
        call.expires_at = datetime.now() + timedelta(milliseconds=timeout_ms)
        ctx.call_map[call.id] = call
        self.schedule(ctx.conn.send(*call.to_call_cmd_argv()))
        return call

    async def cmd_syn(self, conn: Connection):
        await conn.send("ack")

    async def cmd_ack(self, conn: Connection):
        ctx = self._conn_ctx_map[conn.id]
        ctx.ack_ttl = None
        ctx.syn_at = (
            datetime.now()
            + self._syn_schedule
            + timedelta(seconds=random.randint(-self._syn_jitter, self._syn_jitter))
        )

    async def cmd_bye(self, conn: Connection):
        self.disconnect(conn, notify=False)

    async def cmd_call(self, conn: Connection, call_id: str, fn_name: str, *argv):
        try:
            function = self._functions.get(fn_name)
        except ValueError:
            log.debug(
                f"Received call for an undefined function `{fn_name}` id={call_id}"
            )
            await conn.send(
                "return",
                call_id,
                Response.Code.ERROR,
                Response.Errors.UNDEFINED_FUNCTION,
            )
            return

        try:
            result = await async_wrap(function, conn, *argv)

        except Exception as e:
            log.exception("Error processing peer call to function `{fn_name}`.")
            await conn.send(
                "return",
                call_id,
                Response.Code.ERROR,
                Response.Errors.RUNTIME_ERROR,
                str(e),
            )
            return

        if is_iterable(result):
            await conn.send(
                "return", call_id, Response.Code.OK, *[str(r) for r in result]
            )
        else:
            await conn.send("return", call_id, Response.Code.OK, str(result))

    async def cmd_return(
        self, conn: Connection, call_id_str: str, response_code: str, *argv
    ):
        ctx = self._conn_ctx_map[conn.id]

        call_id = int(call_id_str)
        call = ctx.call_map[call_id]
        call.future.set_result(Response(Response.Code(response_code), [*argv]))
        del ctx.call_map[call_id]
