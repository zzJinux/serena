import asyncio
import json
import logging
import os
import signal
import threading
import traceback
from collections.abc import Callable
from pathlib import Path

import anyio
from mcp.server.fastmcp import FastMCP
from mcp.shared.session import SessionMessage
from mcp.types import JSONRPCMessage

from serena.agent import SerenaAgent
from serena.constants import SERENA_LOG_FORMAT
from serena.mcp import SerenaMCPFactory
from serena.project import Project

log = logging.getLogger(__name__)

DAEMON_SOCKET_PATH = Path(os.environ.get("SERENA_DAEMON_SOCKET", Path.home() / ".serena" / "daemon.sock"))


class SharedProjectPool:
    """Ref-counted pool of Project instances keyed by canonical project root."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pool: dict[str, tuple[Project, int]] = {}

    def acquire(self, project_root: str, create_fn: Callable[[], Project]) -> Project:
        key = str(Path(project_root).resolve())
        with self._lock:
            if key in self._pool:
                project, count = self._pool[key]
                self._pool[key] = (project, count + 1)
                return project
            project = create_fn()
            self._pool[key] = (project, 1)
            return project

    def release(self, project_root: str, timeout: float = 2.0) -> None:
        key = str(Path(project_root).resolve())
        with self._lock:
            if key not in self._pool:
                return
            project, count = self._pool[key]
            if count <= 1:
                del self._pool[key]
                project.shutdown(timeout=timeout)
            else:
                self._pool[key] = (project, count - 1)


class AgentManager:
    def __init__(self, enable_gui_log_window: bool = False):
        self._agents: list[tuple[FastMCP, SerenaAgent]] = []
        self._project_pool = SharedProjectPool()
        self.enable_gui_log_window = enable_gui_log_window

    def create_agent(self, project_path: str, context: str, modes: list[str]) -> tuple[FastMCP, SerenaAgent]:
        log.info(f"Creating new agent for {project_path}")

        # Create factory WITHOUT project — defer activation so we can inject the pool first.
        # The MCP server hasn't started yet, so the client never sees the briefly unfiltered tool list.
        factory = SerenaMCPFactory(context=context, project=None)

        mcp_server = factory.create_mcp_server(
            modes=modes,
            enable_web_dashboard=False,
            enable_gui_log_window=self.enable_gui_log_window,
        )
        agent = factory.agent
        if agent is None:
            raise RuntimeError("SerenaMCPFactory.agent is None after create_mcp_server")

        # Inject the shared project pool before activating the project
        agent._project_pool = self._project_pool
        agent.activate_project_from_path_or_name(project_path)

        self._agents.append((mcp_server, agent))

        return mcp_server, agent

    def remove_agent(self, mcp: FastMCP, agent: SerenaAgent) -> None:
        entry = (mcp, agent)
        if entry in self._agents:
            self._agents.remove(entry)
        else:
            log.warning("Attempted to remove agent that was not in the active agents list")
        try:
            agent.shutdown()
        except Exception as e:
            log.exception(f"Error during agent shutdown: {e}")


class AsyncioReadStream:
    def __init__(self, reader: asyncio.StreamReader):
        self.reader = reader

    async def receive(self) -> SessionMessage:
        line = await self.reader.readline()
        if not line:
            raise anyio.EndOfStream

        try:
            # Parse JSON
            data = json.loads(line)
            # Validate and convert to JSONRPCMessage
            msg = JSONRPCMessage.model_validate(data)
            return SessionMessage(message=msg)
        except Exception as e:
            log.error(f"Failed to parse message: {e}")
            # Try next message
            return await self.receive()

    async def aclose(self):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.aclose()

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return await self.receive()
        except anyio.EndOfStream:
            raise StopAsyncIteration


class AsyncioWriteStream:
    def __init__(self, writer: asyncio.StreamWriter):
        self.writer = writer

    async def send(self, item: SessionMessage):
        # Serialize SessionMessage back to JSON bytes
        # item.message is the JSONRPCMessage
        if isinstance(item, SessionMessage):
            data = item.message.model_dump_json().encode() + b"\n"
            self.writer.write(data)
            await self.writer.drain()
        else:
            log.error(f"Unexpected item type in write stream: {type(item)}")

    async def aclose(self):
        self.writer.close()
        await self.writer.wait_closed()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.aclose()


class ConnectionHandler:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, manager: AgentManager):
        self.reader = reader
        self.writer = writer
        self.manager = manager
        self.project_path: str | None = None
        self.context: str = "default"
        self.modes: list[str] = []

    async def handle(self):
        mcp: FastMCP | None = None
        agent: SerenaAgent | None = None
        try:
            # 1. Handshake: Expect JSON with project info
            # We read line by line assuming the client sends a newline-delimited JSON for handshake
            # or just a fixed header.
            # Let's assume the first message is the handshake.

            # Read handshake
            line = await self.reader.readline()
            if not line:
                return

            try:
                handshake = json.loads(line)
                self.project_path = handshake.get("project")
                self.context = handshake.get("context", "default")
                self.modes = handshake.get("modes", [])

                if not self.project_path:
                    raise ValueError("No project path in handshake")

                log.info(f"Handshake successful: {self.project_path}")

                # Send ack
                self.writer.write(json.dumps({"status": "ok"}).encode() + b"\n")
                await self.writer.drain()

            except Exception as e:
                log.error(f"Handshake failed: {e}")
                self.writer.write(json.dumps({"status": "error", "message": str(e)}).encode() + b"\n")
                await self.writer.drain()
                return

            # 2. Create MCP Server
            mcp, agent = self.manager.create_agent(self.project_path, self.context, self.modes)

            # 3. Run MCP Server with custom streams
            read_stream = AsyncioReadStream(self.reader)
            write_stream = AsyncioWriteStream(self.writer)

            # We need to access the underlying mcp server to run it with custom streams
            # FastMCP._mcp_server is the underlying mcp.server.Server
            if hasattr(mcp, "_mcp_server"):
                await mcp._mcp_server.run(read_stream, write_stream, mcp._mcp_server.create_initialization_options())
            else:
                log.error("FastMCP instance does not have _mcp_server attribute")
                raise RuntimeError("Incompatible FastMCP version")

        except Exception as e:
            log.error(f"Connection error: {e}")
            traceback.print_exc()
        finally:
            self.writer.close()
            await self.writer.wait_closed()
            if mcp is not None and agent is not None:
                self.manager.remove_agent(mcp, agent)


class SerenaDaemon:
    def __init__(self, enable_gui_log_window: bool = False):
        self.socket_path = DAEMON_SOCKET_PATH
        self.manager = AgentManager(enable_gui_log_window=enable_gui_log_window)
        self._server: asyncio.AbstractServer | None = None
        self._active_handlers: set[asyncio.Task[None]] = set()
        self._shutting_down = False

    async def start(self):
        # Ensure directory exists
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)

        # Remove existing socket
        if self.socket_path.exists():
            self.socket_path.unlink()

        # Register signal handlers for graceful shutdown
        loop = asyncio.get_running_loop()
        shutdown_callback = lambda: asyncio.create_task(self.shutdown())
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, shutdown_callback)

        self._server = await asyncio.start_unix_server(self.handle_client, str(self.socket_path))

        log.info(f"Daemon listening on {self.socket_path}")

        async with self._server:
            await self._server.serve_forever()

    async def shutdown(self) -> None:
        """Gracefully shutdown daemon and all agents."""
        if self._shutting_down:
            return
        self._shutting_down = True
        log.info("Daemon shutting down...")

        if self._server:
            self._server.close()
            await self._server.wait_closed()

        # Cancel all active handlers
        for task in list(self._active_handlers):
            task.cancel()

        # Wait for handlers to finish with timeout
        if self._active_handlers:
            await asyncio.wait(self._active_handlers, timeout=2.0)

        # Shutdown all agents
        for mcp, agent in list(self.manager._agents):
            self.manager.remove_agent(mcp, agent)

        # Clean up socket file
        if self.socket_path.exists():
            self.socket_path.unlink()

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        handler = ConnectionHandler(reader, writer, self.manager)
        task = asyncio.current_task()
        if task:
            self._active_handlers.add(task)
        try:
            await handler.handle()
        finally:
            if task:
                self._active_handlers.discard(task)


def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO, format=SERENA_LOG_FORMAT)

    daemon = SerenaDaemon()
    try:
        asyncio.run(daemon.start())
    except KeyboardInterrupt:
        log.info("Received KeyboardInterrupt, shutting down...")
    finally:
        # Ensure socket is cleaned up even if shutdown wasn't called
        if daemon.socket_path.exists():
            daemon.socket_path.unlink()


if __name__ == "__main__":
    main()
