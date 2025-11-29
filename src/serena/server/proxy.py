import asyncio
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

from serena.constants import SERENA_LOG_FORMAT

log = logging.getLogger(__name__)

DAEMON_SOCKET_PATH = Path(os.environ.get("SERENA_DAEMON_SOCKET", Path.home() / ".serena" / "daemon.sock"))
DEBUG_LOG_PATH = Path("/tmp/serena_proxy.log")


def debug_log(msg):
    with open(DEBUG_LOG_PATH, "a") as f:
        f.write(f"{time.time()}: {msg}\n")


class SerenaProxy:
    def __init__(self, project_path: str, context: str, modes: list[str]):
        self.project_path = str(Path(project_path).resolve())
        self.context = context
        self.modes = modes
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None

    async def connect(self):
        # Try to connect to daemon
        for i in range(3):
            try:
                self.reader, self.writer = await asyncio.open_unix_connection(str(DAEMON_SOCKET_PATH))
                return
            except (FileNotFoundError, ConnectionRefusedError):
                if i == 0:
                    debug_log("Daemon not running, starting it...")
                    self._start_daemon()
                    # Wait for daemon to start
                    await asyncio.sleep(1)
                else:
                    await asyncio.sleep(1)

        raise ConnectionError("Could not connect to Serena Daemon")

    def _start_daemon(self):
        # Start daemon process detached
        # We assume 'serena' command is available or we use sys.executable
        cmd = [sys.executable, "-m", "serena.server.daemon"]
        debug_log(f"Starting daemon with {cmd}")
        with open("/tmp/serena_daemon.out", "w") as out, open("/tmp/serena_daemon.err", "w") as err:
            subprocess.Popen(cmd, start_new_session=True, stdout=out, stderr=err, stdin=subprocess.DEVNULL)

    async def run(self):
        await self.connect()
        assert self.reader is not None
        assert self.writer is not None

        # Handshake
        handshake = {"project": self.project_path, "context": self.context, "modes": self.modes}
        self.writer.write(json.dumps(handshake).encode() + b"\n")
        await self.writer.drain()

        # Read handshake response
        response_line = await self.reader.readline()
        if not response_line:
            raise ConnectionError("Daemon closed connection during handshake")

        response = json.loads(response_line)
        if response.get("status") != "ok":
            raise ConnectionError(f"Handshake failed: {response.get('message')}")

        debug_log("Connected to Serena Daemon")

        # Start forwarding loops
        await asyncio.gather(self._forward_stdin_to_socket(), self._forward_socket_to_stdout())

    async def _forward_stdin_to_socket(self):
        debug_log("Starting stdin forwarding")
        loop = asyncio.get_event_loop()
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        await loop.connect_read_pipe(lambda: protocol, sys.stdin)

        while True:
            data = await reader.read(4096)
            if not data:
                debug_log("Stdin closed")
                break
            self.writer.write(data)
            await self.writer.drain()

        self.writer.close()

    async def _forward_socket_to_stdout(self):
        debug_log("Starting socket forwarding")
        while True:
            data = await self.reader.read(4096)
            if not data:
                debug_log("Socket closed")
                break
            sys.stdout.buffer.write(data)
            sys.stdout.buffer.flush()


if __name__ == "__main__":
    # Test run
    logging.basicConfig(level=logging.INFO, format=SERENA_LOG_FORMAT)
    proxy = SerenaProxy(os.getcwd(), "default", [])
    asyncio.run(proxy.run())
