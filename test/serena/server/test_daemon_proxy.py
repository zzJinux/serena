import asyncio
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from serena.server.daemon import AgentManager, SerenaDaemon
from serena.server.proxy import SerenaProxy


# Mock FastMCP to avoid actual server creation
@pytest.fixture
def mock_fastmcp():
    with patch("serena.server.daemon.FastMCP") as mock:
        yield mock


@pytest.fixture
def mock_factory():
    with patch("serena.server.daemon.SerenaMCPFactory") as mock:
        yield mock


@pytest.mark.asyncio
async def test_agent_manager_create_agent(mock_factory, mock_fastmcp):
    manager = AgentManager()

    # Setup mocks
    factory_instance = mock_factory.return_value
    mcp_server = MagicMock()
    serena_agent = MagicMock()
    factory_instance.create_mcp_server.return_value = mcp_server
    factory_instance.agent = serena_agent

    mcp, agent = manager.create_agent("/tmp/project", "default", [])

    assert mcp == mcp_server
    assert agent == serena_agent
    assert len(manager._agents) == 1
    assert manager._agents[0] == (mcp_server, serena_agent)
    mock_factory.assert_called_with(context="default", project="/tmp/project")
    factory_instance.create_mcp_server.assert_called_once()


@pytest.mark.asyncio
async def test_agent_manager_remove_agent(mock_factory, mock_fastmcp):
    manager = AgentManager()

    # Setup mocks
    factory_instance = mock_factory.return_value
    mcp_server = MagicMock()
    serena_agent = MagicMock()
    factory_instance.create_mcp_server.return_value = mcp_server
    factory_instance.agent = serena_agent

    mcp, agent = manager.create_agent("/tmp/project", "default", [])
    assert len(manager._agents) == 1

    # Test removal
    manager.remove_agent(mcp, agent)
    assert len(manager._agents) == 0
    agent.shutdown.assert_called_once()


@pytest.mark.asyncio
async def test_agent_manager_remove_agent_handles_shutdown_error(mock_factory, mock_fastmcp):
    manager = AgentManager()

    # Setup mocks
    factory_instance = mock_factory.return_value
    mcp_server = MagicMock()
    serena_agent = MagicMock()
    serena_agent.shutdown.side_effect = Exception("Shutdown failed")
    factory_instance.create_mcp_server.return_value = mcp_server
    factory_instance.agent = serena_agent

    mcp, agent = manager.create_agent("/tmp/project", "default", [])
    assert len(manager._agents) == 1

    # Test removal - should not raise even if shutdown fails
    manager.remove_agent(mcp, agent)
    assert len(manager._agents) == 0
    agent.shutdown.assert_called_once()


@pytest.mark.asyncio
async def test_daemon_handshake():
    # Create a temporary socket path
    with tempfile.TemporaryDirectory() as tmpdir:
        socket_path = Path(tmpdir) / "daemon.sock"

        # Start Daemon
        daemon = SerenaDaemon()
        daemon.socket_path = socket_path

        # Mock AgentManager to avoid creating real agents
        daemon.manager = MagicMock()
        mock_mcp = MagicMock()
        mock_serena_agent = MagicMock()
        # Mock _mcp_server attribute for the mcp
        mock_mcp._mcp_server = MagicMock()
        mock_mcp._mcp_server.run = AsyncMock()
        mock_mcp._mcp_server.create_initialization_options = MagicMock()
        daemon.manager.create_agent.return_value = (mock_mcp, mock_serena_agent)

        # Start daemon in background
        server_task = asyncio.create_task(daemon.start())

        # Wait for socket
        while not socket_path.exists():
            await asyncio.sleep(0.1)

        # Connect with Proxy logic (manually)
        reader, writer = await asyncio.open_unix_connection(str(socket_path))

        # Send handshake
        handshake = {"project": "/tmp/test_project", "context": "default", "modes": []}
        writer.write(json.dumps(handshake).encode() + b"\n")
        await writer.drain()

        # Read response
        response_line = await reader.readline()
        response = json.loads(response_line)

        assert response["status"] == "ok"

        # Verify agent creation
        daemon.manager.create_agent.assert_called_with("/tmp/test_project", "default", [])

        # Clean up
        writer.close()
        await writer.wait_closed()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_proxy_connect_and_handshake():
    with tempfile.TemporaryDirectory() as tmpdir:
        socket_path = Path(tmpdir) / "daemon.sock"

        # Mock Daemon environment variable
        with patch.dict(os.environ, {"SERENA_DAEMON_SOCKET": str(socket_path)}):
            # Start a simple mock daemon server
            async def handle_client(reader, writer):
                # Read handshake
                line = await reader.readline()
                data = json.loads(line)
                assert data["project"] == "/tmp/proxy_test"

                # Send OK
                writer.write(json.dumps({"status": "ok"}).encode() + b"\n")
                await writer.drain()
                writer.close()

            server = await asyncio.start_unix_server(handle_client, str(socket_path))

            # Start Proxy
            proxy = SerenaProxy("/tmp/proxy_test", "default", [])

            await proxy.connect()

            # Manually perform handshake part of run() to verify
            # We can't call proxy.run() easily because it enters infinite loops
            # So we test the connection and handshake logic by simulating what run() does

            assert proxy.reader is not None
            assert proxy.writer is not None

            handshake = {"project": proxy.project_path, "context": proxy.context, "modes": proxy.modes}
            proxy.writer.write(json.dumps(handshake).encode() + b"\n")
            await proxy.writer.drain()

            response_line = await proxy.reader.readline()
            response = json.loads(response_line)
            assert response["status"] == "ok"

            server.close()
            await server.wait_closed()
