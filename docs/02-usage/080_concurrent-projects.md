# Concurrent Projects

Serena supports running multiple projects concurrently, allowing you to work with different codebases simultaneously without conflicts. This is achieved through a daemon-based architecture that manages multiple agent instances.

## Architecture Overview

The concurrent projects feature uses a client-server architecture:

- **Daemon (Mother Server)**: A background process (`SerenaDaemon`) that manages multiple `SerenaAgent` instances, one per project
- **Session Proxy**: A lightweight client process that connects to the daemon and forwards JSON-RPC messages between your MCP client (e.g., Claude Desktop) and the appropriate agent

## How It Works

When you run `serena start-mcp-server` with the default `stdio` transport:

1. The command starts a **Session Proxy** instead of a full MCP server
2. The proxy identifies your project (based on the current directory or `--project` argument)
3. The proxy attempts to connect to the daemon via a Unix Domain Socket
4. The proxy performs a handshake with the daemon, sending the project path
5. The daemon creates or retrieves a `SerenaAgent` for your project
6. All JSON-RPC messages are forwarded between your client and the daemon

**Important**: You must start the daemon manually using `serena-daemon` before running any sessions.

## Usage

### Basic Usage

Simply run Serena as usual. The concurrent projects feature is enabled automatically when using `stdio` transport:

```bash
# 1. Start the daemon (in a separate terminal or background)
serena-daemon

# 2. Start your project sessions
# In project A
cd /path/to/project-a
serena start-mcp-server

# In project B (in a different terminal)
cd /path/to/project-b
serena start-mcp-server
```

Both sessions will run concurrently without interference.

### With Claude Desktop

Configure multiple projects in your Claude Desktop MCP settings:

```json
{
  "mcpServers": {
    "serena-project-a": {
      "command": "serena",
      "args": ["start-mcp-server", "--project", "/path/to/project-a"]
    },
    "serena-project-b": {
      "command": "serena",
      "args": ["start-mcp-server", "--project", "/path/to/project-b"]
    }
  }
}
```

Each configured server will connect to the same daemon but maintain separate agent instances.

## Configuration

### Socket Path

By default, the daemon listens on `~/.serena/daemon.sock`. You can customize this using the `SERENA_DAEMON_SOCKET` environment variable:

```bash
export SERENA_DAEMON_SOCKET=/tmp/my-serena-daemon.sock
serena start-mcp-server
```

**Note**: All proxies must use the same socket path to connect to the same daemon.

## Process Management

### Checking Running Processes

To see the daemon and proxy processes:

```bash
ps aux | grep serena
```

You should see:
- One `serena.server.daemon` process (the daemon)
- Multiple `serena` processes (one proxy per active session)

### Stopping the Daemon

The daemon runs in the background. To stop it:

```bash
pkill -f serena.server.daemon
```

Or find and kill the specific process:

```bash
ps aux | grep serena.server.daemon
kill <PID>
```

The daemon will automatically restart when you run a new session.

### Cleaning Up

To remove the socket file:

```bash
rm ~/.serena/daemon.sock
```

## Dashboard Access

Each `SerenaAgent` running in the daemon spawns its own web dashboard on a different port. The port is automatically assigned and logged when the agent starts.

To find the dashboard ports, check the daemon logs:

```bash
cat /tmp/serena_daemon.err
```

Look for lines like:
```
INFO  ... serena.dashboard:... - Dashboard running on port 12345
```

## Limitations and Considerations

### Transport Compatibility

The concurrent projects feature only works with `stdio` transport. If you use `sse` or `streamable-http` transports, Serena will run in the traditional single-process mode.

### Resource Usage

Each active project maintains its own:
- Language server instances
- File system watchers
- Memory caches

Monitor your system resources when running many concurrent projects.

### Dashboard Consolidation

Currently, each project has its own dashboard. Future versions may consolidate these into a single unified dashboard.

## Troubleshooting

### Connection Refused

If you see "Could not connect to Serena Daemon":

1. Check if the daemon is running: `ps aux | grep serena.server.daemon`
2. Verify the socket path exists: `ls -la ~/.serena/daemon.sock`
3. Check daemon logs: `cat /tmp/serena_daemon.err`
4. Try manually starting the daemon: `python -m serena.server.daemon`

### Socket Permission Issues

If you encounter permission errors:

```bash
chmod 700 ~/.serena
chmod 600 ~/.serena/daemon.sock
```

### Stale Socket File

If the daemon crashed, the socket file may remain:

```bash
rm ~/.serena/daemon.sock
# Then restart your session
```

### Debug Logging

Enable debug logging for the proxy:

```bash
# The proxy logs to /tmp/serena_proxy.log
tail -f /tmp/serena_proxy.log
```

## Advanced Usage

### Custom Daemon Management

For advanced use cases, you can manually manage the daemon:

```bash
# Start daemon explicitly
python -m serena.server.daemon

# Use a custom socket path
export SERENA_DAEMON_SOCKET=/custom/path/daemon.sock
python -m serena.server.daemon

# Start with debug logging
serena-daemon --log-level DEBUG

# Start with GUI log window
serena-daemon --enable-gui-log-window
```

### Testing Concurrent Sessions

To verify concurrent sessions are working:

```bash
# Terminal 1
cd /path/to/project-a
serena start-mcp-server

# Terminal 2
cd /path/to/project-b
serena start-mcp-server

# Check processes
ps aux | grep serena
# Should show 1 daemon + 2 proxies
```

## Technical Details

For developers interested in the implementation:

- **Daemon**: `src/serena/server/daemon.py`
- **Proxy**: `src/serena/server/proxy.py`
- **Tests**: `test/serena/server/test_daemon_proxy.py`

The daemon uses custom `AsyncioReadStream` and `AsyncioWriteStream` classes to bridge asyncio socket streams with the MCP library's stream interface.
