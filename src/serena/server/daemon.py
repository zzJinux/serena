import asyncio
import json
import logging
import os
import traceback
from pathlib import Path

import anyio
from mcp.server.fastmcp import FastMCP
from mcp.shared.session import SessionMessage
from mcp.types import JSONRPCMessage

from serena.agent import SerenaAgent
from serena.constants import SERENA_LOG_FORMAT
from serena.mcp import SerenaMCPFactory

log = logging.getLogger(__name__)

DAEMON_SOCKET_PATH = Path(os.environ.get("SERENA_DAEMON_SOCKET", Path.home() / ".serena" / "daemon.sock"))


class AgentManager:
    def __init__(self, enable_gui_log_window: bool = False):
        self._agents: list[tuple[FastMCP, SerenaAgent]] = []
        self.enable_gui_log_window = enable_gui_log_window

    def create_agent(self, project_path: str, context: str, modes: list[str]) -> tuple[FastMCP, SerenaAgent]:
        log.info(f"Creating new agent for {project_path}")

        # Create factory and instantiate the agent.
        # We use SerenaMCPFactory to create the FastMCP instance.
        # The actual execution (run) will be handled by the ConnectionHandler using custom streams.
        factory = SerenaMCPFactory(context=context, project=project_path)

        mcp_server = factory.create_mcp_server(
            modes=modes,
            enable_web_dashboard=False,
            enable_gui_log_window=self.enable_gui_log_window,
        )
        agent = factory.agent
        if agent is None:
            raise RuntimeError("SerenaMCPFactory.agent is None after create_mcp_server")
        self._agents.append((mcp_server, agent))

        return mcp_server, agent

    def remove_agent(self, mcp: FastMCP, agent: SerenaAgent) -> None:
        entry = (mcp, agent)
        if entry in self._agents:
            self._agents.remove(entry)
        try:
            agent.shutdown()
        except Exception as e:
            log.error(f"Error during agent shutdown: {e}", exc_info=True)


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
            if mcp is not None and agent is not None:
                self.manager.remove_agent(mcp, agent)


class SerenaDaemon:
    def __init__(self, enable_gui_log_window: bool = False):
        self.socket_path = DAEMON_SOCKET_PATH
        self.manager = AgentManager(enable_gui_log_window=enable_gui_log_window)

    async def start(self):
        # Ensure directory exists
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)

        # Remove existing socket
        if self.socket_path.exists():
            self.socket_path.unlink()

        server = await asyncio.start_unix_server(self.handle_client, str(self.socket_path))

        log.info(f"Daemon listening on {self.socket_path}")

        async with server:
            await server.serve_forever()

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        handler = ConnectionHandler(reader, writer, self.manager)
        await handler.handle()


def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO, format=SERENA_LOG_FORMAT)

    daemon = SerenaDaemon()
    try:
        asyncio.run(daemon.start())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
