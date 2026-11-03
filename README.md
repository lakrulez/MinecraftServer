# BedrockNexus — Minecraft Bedrock Multi-Server Manager & LAN Discovery Proxy

A self-hosted, lightweight **Minecraft Bedrock Multi-Server Manager** with a dark-mode Web GUI, real-time WebSocket console & telemetry, world backup/import automation, and an embedded **dual-protocol RakNet (`UDP 19132`) + NetherNet (`UDP 7551`) LAN Discovery Proxy** for simultaneous multi-server LAN discovery across mobile, PC, and consoles (Nintendo Switch / PlayStation / Xbox).

---

## Key Features

- **Multi-Server Instance Management:**
  - Create and run multiple official Mojang **Bedrock Dedicated Server (`bedrock_server`)** instances simultaneously from a single shared binary directory (`bds_bin/`).
  - Automatic unique port allocation (`19133`, `19134`, `19135`, ...) and automatic `server.properties` generation per instance.
  - Per-instance **Start**, **Graceful Stop**, **Restart**, and **Force Kill** controls with live CPU %, RAM usage, uptime, and player count telemetry.
- **Simultaneous Multi-Server LAN Discovery (`UDP 7551` NetherNet & `UDP 19132` RakNet):**
  - **NetherNet (`UDP 7551`) Proxy (Bedrock 1.21+ / 1.26+):** Fans out LAN discovery requests (`DiscoveryRequestPacket`) to all running instances simultaneously so **every online server appears at the same time** in the Minecraft **Worlds -> LAN Games** tab, and routes WebRTC connection signaling (`DiscoveryMessagePacket`) to the exact server clicked by inspecting the 64-bit `recipient_network_id`.
  - **RakNet (`UDP 19132`) Console Bridge:** Responds to `ID_UNCONNECTED_PING` (`0x01`/`0x02`) with `ID_UNCONNECTED_PONG` (`0x1c`) and transparently bridges console traffic to the selected primary instance.
- **Real-Time Interactive Console:**
  - Live WebSocket-streamed server console logs with ANSI color rendering.
  - Interactive `STDIN` command bar and one-click quick actions (`time set day`, `weather clear`, `gamemode`, `op`, `save-all`, `list`).
- **World Backup, Restore & `.mcworld` Import:**
  - One-click hot backups using BDS `save hold` -> `save query` -> `.zip` archive -> `save resume`.
  - One-click backup restore and `.mcworld` / `.zip` world import directly from the Web GUI.

---

## Project Structure

```text
MinecraftServer/
├── app/
│   ├── main.py          # FastAPI backend, REST API & WebSocket hub
│   ├── manager.py       # Instance supervisor, SQLite store, port allocator & backup manager
│   ├── broadcaster.py   # Async UDP RakNet (19132) & NetherNet (7551) LAN Discovery Proxy
│   ├── bds_runtime.py   # Built-in fallback BDS-compatible simulator engine
│   └── static/
│       └── index.html   # Responsive dark-mode Web GUI dashboard (Tailwind CSS + Alpine.js)
├── bds_bin/             # Official Mojang Bedrock Dedicated Server binary & asset packs (gitignored)
├── instances/           # Per-instance isolated server directories & worlds (gitignored)
├── backups/             # Timestamped world backup .zip archives (gitignored)
├── data/                # SQLite database (instances.db) (gitignored)
├── requirements.txt     # Python dependencies
├── start.bat            # Windows one-click launcher
├── start.ps1            # PowerShell launcher
└── start.sh             # Linux/macOS launcher
```

---

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Add Official Bedrock Dedicated Server Binary (Optional but Recommended)
Download the official Mojang Bedrock Dedicated Server zip for Windows/Linux from [minecraft.net/en-us/download/server/bedrock](https://www.minecraft.net/en-us/download/server/bedrock) and extract it into `bds_bin/` so that `bds_bin/bedrock_server.exe` (or `bds_bin/bedrock_server` on Linux) exists.

> If `bds_bin/` is not present, BedrockNexus automatically runs its built-in BDS-compatible fallback engine (`app/bds_runtime.py`) for testing.

### 3. Launch the Manager
- **Windows (Command Prompt / Double-Click):**
  ```cmd
  start.bat
  ```
- **Windows (PowerShell):**
  ```powershell
  powershell -ExecutionPolicy Bypass -File .\start.ps1
  ```
- **Linux / macOS:**
  ```bash
  chmod +x start.sh && ./start.sh
  ```

Open **`http://localhost:8000`** in your browser.
