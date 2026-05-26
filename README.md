# StarCraft LAN Host Relay

Experimental Python implementation of a non-player LAN host service for the
StarCraft/Brood War UDP protocol seen in `host2.pcapng`.

Run:

```powershell
.\start_host.bat
```

If Python is installed somewhere else, set `SC_HOST_PYTHON` before starting:

```powershell
$env:SC_HOST_PYTHON = "C:\Path\To\python.exe"
.\start_host.ps1
```

The first version is intentionally narrow:

- `SEXP / 0xC3`
- UDP `6111` room discovery and UDP `6112` Storm traffic
- fixed 1v1 room
- fixed Challenger room/map metadata
- central relay: real clients talk to Python, not to each other
- periodic LAN room advertisements, plus replies to client search broadcasts

This is a protocol prototype, not a hardened production server. The game logic is
not simulated; once in game, the service relays `CLS=2` command payloads between
the two clients.
