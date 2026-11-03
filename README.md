# BedrockNexus — Self-Healing Minecraft Bedrock Multi-Server Manager & LAN Discovery Proxy

A self-hosted, zero-config **Minecraft Bedrock Multi-Server Manager** with a dark-mode Web GUI, real-time WebSocket console & telemetry, world backup/import automation, **automatic dependency & binary bootstrapper**, and a **self-testing / self-correcting RakNet (`UDP 19132`) + NetherNet (`UDP 7551`) LAN Discovery Proxy** for simultaneous multi-server LAN discovery across mobile, PC, and consoles (Nintendo Switch / PlayStation / Xbox).

---

## Key Features

- **Zero-Touch Auto-Bootstrapper (`app/bootstrap.py` & `redist/`):**
  - **Bundled Visual C++ Runtime DLLs (`redist/`):** Includes all required Microsoft VC++ 140 x64 runtime DLLs (`msvcp140.dll`, `vcruntime140.dll`, `vcruntime140_1.dll`, `concrt140.dll`, etc.) directly in the repository and automatically copies them into `bds_bin/` and every instance folder—plus auto-downloads & installs `vc_redist.x64.exe` if needed—so `bedrock_server.exe` never fails with `0xC0000135` on a fresh Windows PC.
  - **Automatic Official Mojang BDS Download:** Automatically downloads and extracts the official Mojang Bedrock Dedicated Server (`bedrock-server-1.26.61.1.zip`) into `bds_bin/` on first run if `bds_bin/bedrock_server.exe` is missing.
  - **Automatic Windows Firewall Configuration:** Removes any accidental Windows Firewall `Block` rules on `python.exe` or `bedrock_server.exe` and opens inbound/outbound LAN discovery rules across `Domain`, `Private`, and `Public` profiles (`UDP 7551`, `UDP 19132-19165`, `UDP 20132-20165`, `UDP 49152-65535`, `TCP 8000`).
- **Continuous Self-Test & Auto-Healing Engine (`SelfHealingCoordinator`):**
  - Every 8 seconds (and on-demand via the **"Self-Test & Auto-Heal"** button in the Web GUI), BedrockNexus sends real encrypted **NetherNet `DiscoveryRequestPacket` (`UDP 7551`)** and **RakNet `ID_UNCONNECTED_PING` (`UDP 19132`)** probes to itself, decrypts the responses, and verifies that **every running server instance is actively responding and visible in the Minecraft LAN Worlds list**.
  - If any instance fails to respond, binds the wrong port, or has corrupted stub world files, BedrockNexus automatically repairs the world/ports, rebinds the proxy sockets, or restarts the unresponsive instance.
- **Simultaneous Multi-Server LAN Discovery (`UDP 7551` NetherNet & `UDP 19132` RakNet):**
  - **NetherNet (`UDP 7551`) Proxy (Bedrock 1.21+ / 1.26+):** Fans out LAN discovery requests (`DiscoveryRequestPacket`) to all running instances simultaneously so **every online server appears at the same time** in the Minecraft **Worlds -> LAN Games** tab, and routes WebRTC connection signaling (`DiscoveryMessagePacket`) to the exact server clicked by inspecting the 64-bit `recipient_network_id`.
  - **RakNet (`UDP 19132`) Console Bridge:** Responds to `ID_UNCONNECTED_PING` (`0x01`/`0x02`) with `ID_UNCONNECTED_PONG` (`0x1c`) and transparently bridges console traffic to the selected primary instance.
- **Real-Time Interactive Console & World Backup Automation:**
  - Live WebSocket-streamed server console logs with ANSI color rendering and interactive `STDIN` command bar.
  - One-click hot backups (`save hold` -> `save query` -> `.zip` archive -> `save resume`), restore, and `.mcworld` import.

---

## Project Structure

```text
MinecraftServer/
├── app/
│   ├── main.py          # FastAPI backend, REST API, WebSocket hub & background self-healer
│   ├── bootstrap.py     # Auto-downloader (BDS + VC++ Redist), Firewall fixer & Self-Test engine
│   ├── manager.py       # Instance supervisor, SQLite store, port allocator & backup manager
│   ├── broadcaster.py   # Async UDP RakNet (19132) & NetherNet (7551) LAN Discovery Proxy
│   ├── bds_runtime.py   # Built-in fallback BDS-compatible simulator engine
│   └── static/
│       └── index.html   # Responsive dark-mode Web GUI dashboard with Live Self-Test bar
├── redist/              # Bundled Microsoft VC++ 2015-2022 x64 runtime DLLs (committed to git)
├── bds_bin/             # Official Mojang Bedrock Dedicated Server binary (auto-downloaded)
├── instances/           # Per-instance isolated server directories & worlds (gitignored)
├── backups/             # Timestamped world backup .zip archives (gitignored)
├── data/                # SQLite database (instances.db) (gitignored)
├── requirements.txt     # Python dependencies (fastapi, uvicorn, psutil, cryptography)
├── start.bat            # Windows one-click launcher (double-click to setup & run)
├── start.ps1            # PowerShell auto-bootstrapping launcher
└── start.sh             # Linux/macOS launcher
```

---

## One-Click Quick Start (Fresh PC or Laptop)

1. Clone or download & extract the repository ZIP from GitHub.
2. Double-click **`start.bat`** (or run `powershell -ExecutionPolicy Bypass -File .\start.ps1`).
   - Automatically installs Python 3.12 (if missing), Python libraries, Visual C++ Runtime DLLs, Mojang `bedrock_server.exe`, and Windows Firewall rules, then starts the servers and Web GUI.
3. Open **`http://localhost:8000`** in your browser.
