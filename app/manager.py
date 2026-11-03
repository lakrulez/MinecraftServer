"""
Instance Supervisor, Port Allocator, SQLite Store & World Backup Manager (`/app/manager.py`).

Responsibilities:
- SQLite persistence for server instances and broadcaster settings.
- Automatic unique UDP port allocation starting at 19133 (reserving 19132 for the LAN Proxy).
- Automatic generation and synchronization of `server.properties` for each instance.
- Direct `asyncio.subprocess` supervision of official Bedrock Dedicated Server binaries
  (with seamless fallback to the built-in `app.bds_runtime` BDS-compatible engine).
- Real-time CPU %, RAM (MB), and uptime telemetry via `psutil`.
- World backup automation (`save hold` -> `save query` -> zip -> `save resume`) and
  `.mcworld` / `.zip` world import & restore.
"""

from __future__ import annotations

import asyncio
from collections import deque
import datetime
import io
import json
import logging
import os
from pathlib import Path
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
import uuid
import zipfile
from typing import Any, Awaitable, Callable

import psutil

from app.bds_runtime import ensure_world_files

logger = logging.getLogger("bedrock.manager")

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
INSTANCES_DIR = BASE_DIR / "instances"
BACKUPS_DIR = BASE_DIR / "backups"
BDS_BIN_DIR = BASE_DIR / "bds_bin"
DB_PATH = DATA_DIR / "instances.db"

DEFAULT_START_PORT = 19133
CONSOLE_BUFFER_LIMIT = 600


def format_uptime(seconds: float) -> str:
    if seconds <= 0:
        return "00s"
    total = int(seconds)
    hrs, rem = divmod(total, 3600)
    mins, secs = divmod(rem, 60)
    if hrs > 0:
        return f"{hrs:02d}h {mins:02d}m {secs:02d}s"
    if mins > 0:
        return f"{mins:02d}m {secs:02d}s"
    return f"{secs:02d}s"


def format_bytes(size: int) -> str:
    if size < 1024:
        return f"{size} B"
    if size < 1024 * 1024:
        return f"{size / 1024:.1f} KB"
    return f"{size / (1024 * 1024):.2f} MB"


def is_udp_port_free(port: int) -> bool:
    """Check whether a local UDP port can be bound."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind(("0.0.0.0", port))
            return True
    except OSError:
        return False


class ManagedInstanceRuntime:
    """Tracks live subprocess, console ring buffer, and telemetry for one BDS instance."""

    def __init__(self, instance_id: str) -> None:
        self.instance_id = instance_id
        self.process: asyncio.subprocess.Process | None = None
        self.ps_proc: psutil.Process | None = None
        self.status: str = "offline"  # offline | starting | online | stopping
        self.started_at: float | None = None
        self.engine_mode: str = "BDS Runtime"
        self.logs: deque[dict[str, Any]] = deque(maxlen=CONSOLE_BUFFER_LIMIT)
        self.reader_task: asyncio.Task[None] | None = None
        self.waiter_task: asyncio.Task[None] | None = None
        self.save_ready_event: asyncio.Event = asyncio.Event()
        self.players_online: int = 0
        self.cpu_percent: float = 0.0
        self.memory_mb: float = 0.0
        self.lock = asyncio.Lock()


class InstanceManager:
    """Supervises multiple Bedrock Dedicated Server instances, ports, configs, and backups."""

    def __init__(
        self,
        console_Push_callback: Callable[[str, dict[str, Any]], Awaitable[None]] | None = None,
        state_change_callback: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.console_push_callback = console_Push_callback
        self.state_change_callback = state_change_callback
        self.runtimes: dict[str, ManagedInstanceRuntime] = {}

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        INSTANCES_DIR.mkdir(parents=True, exist_ok=True)
        BACKUPS_DIR.mkdir(parents=True, exist_ok=True)
        BDS_BIN_DIR.mkdir(parents=True, exist_ok=True)

        self._init_db()
        self._seed_default_instances_if_empty()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._get_conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS instances (
                    id TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    world_name TEXT NOT NULL,
                    seed TEXT NOT NULL DEFAULT '',
                    difficulty TEXT NOT NULL DEFAULT 'normal',
                    gamemode TEXT NOT NULL DEFAULT 'survival',
                    max_players INTEGER NOT NULL DEFAULT 10,
                    port INTEGER NOT NULL UNIQUE,
                    ipv6_port INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def _seed_default_instances_if_empty(self) -> None:
        with self._get_conn() as conn:
            count = conn.execute("SELECT COUNT(*) FROM instances").fetchone()[0]
        if count == 0:
            first = self.create_instance(
                display_name="Survival Realm",
                world_name="SurvivalOverworld",
                seed="8675309",
                difficulty="normal",
                gamemode="survival",
                max_players=10,
            )
            self.create_instance(
                display_name="Creative Lab",
                world_name="FlatBuilders",
                seed="2026",
                difficulty="peaceful",
                gamemode="creative",
                max_players=16,
            )
            self.set_setting("broadcaster_enabled", "true")
            self.set_setting("broadcaster_instance_id", first["id"])

    def get_setting(self, key: str, default: str = "") -> str:
        with self._get_conn() as conn:
            row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
            return str(row["value"]) if row else default

    def set_setting(self, key: str, value: str) -> None:
        with self._get_conn() as conn:
            conn.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
            conn.commit()

    def allocate_port(self) -> int:
        """Allocate the next unique internal UDP port (>= 19133)."""
        with self._get_conn() as conn:
            rows = conn.execute("SELECT port, ipv6_port FROM instances").fetchall()
        used_ports = {int(r["port"]) for r in rows} | {int(r["ipv6_port"]) for r in rows}
        used_ports.add(19132)  # Reserved for RakNet LAN Broadcaster

        candidate = DEFAULT_START_PORT
        while candidate < 65000:
            if candidate not in used_ports and is_udp_port_free(candidate):
                return candidate
            candidate += 1
        raise RuntimeError("No available UDP ports found in range 19133-65000")

    def instance_dir(self, instance_id: str) -> Path:
        return INSTANCES_DIR / instance_id

    def world_dir(self, instance_id: str, world_name: str) -> Path:
        return self.instance_dir(instance_id) / "worlds" / world_name

    def has_official_bds(self) -> bool:
        exe_name = "bedrock_server.exe" if os.name == "nt" else "bedrock_server"
        return (BDS_BIN_DIR / exe_name).exists()

    def write_server_properties(self, instance: dict[str, Any]) -> Path:
        """Write standard Bedrock Dedicated Server `server.properties` for an instance."""
        inst_dir = self.instance_dir(instance["id"])
        inst_dir.mkdir(parents=True, exist_ok=True)
        props_path = inst_dir / "server.properties"

        content = (
            "# Minecraft Bedrock Dedicated Server Properties\n"
            "# Managed automatically by Bedrock Multi-Server Manager\n"
            f"server-name={instance['display_name']}\n"
            f"gamemode={instance['gamemode']}\n"
            "force-gamemode=false\n"
            f"difficulty={instance['difficulty']}\n"
            "allow-cheats=true\n"
            f"max-players={instance['max_players']}\n"
            "online-mode=false\n"
            "allow-list=false\n"
            f"server-port={instance['port']}\n"
            f"server-portv6={instance['ipv6_port']}\n"
            "enable-lan-visibility=true\n"
            "transport=nethernet\n"
            "view-distance=32\n"
            "tick-distance=4\n"
            "player-idle-timeout=30\n"
            "max-threads=8\n"
            f"level-name={instance['world_name']}\n"
            f"level-seed={instance.get('seed', '')}\n"
            "default-player-permission-level=member\n"
            "texturepack-required=false\n"
            "content-log-file-enabled=false\n"
            "compression-threshold=1\n"
            "compression-algorithm=zlib\n"
            "server-authoritative-movement=server-auth\n"
            "player-movement-score-threshold=20\n"
            "player-movement-action-direction-threshold=0.85\n"
            "player-movement-distance-threshold=0.3\n"
            "player-movement-duration-threshold-in-ms=500\n"
            "correct-player-movement=false\n"
            "server-authoritative-block-breaking=false\n"
        )
        props_path.write_text(content, encoding="utf-8")

        for json_file in ("permissions.json", "allowlist.json"):
            jf = inst_dir / json_file
            if not jf.exists():
                jf.write_text("[]", encoding="utf-8")

        if not self.has_official_bds():
            ensure_world_files(inst_dir, instance["world_name"], instance.get("seed", ""))
        else:
            w_dir = inst_dir / "worlds" / instance["world_name"]
            w_dir.mkdir(parents=True, exist_ok=True)
            (w_dir / "levelname.txt").write_text(instance["world_name"], encoding="utf-8")
        return props_path

    def create_instance(
        self,
        display_name: str,
        world_name: str,
        seed: str = "",
        difficulty: str = "normal",
        gamemode: str = "survival",
        max_players: int = 10,
        port: int | None = None,
    ) -> dict[str, Any]:
        """Create a new BDS instance, allocate its UDP port, and write `server.properties`."""
        clean_name = display_name.strip() or "Bedrock Server"
        clean_world = re.sub(r'[<>:"/\\|?*]', "_", world_name.strip() or "BedrockLevel")
        clean_diff = difficulty.lower().strip()
        if clean_diff not in ("peaceful", "easy", "normal", "hard"):
            clean_diff = "normal"
        clean_gm = gamemode.lower().strip()
        if clean_gm not in ("survival", "creative", "adventure", "spectator"):
            clean_gm = "survival"
        clean_max = max(1, min(int(max_players), 200))

        allocated_port = int(port) if port else self.allocate_port()
        ipv6_port = allocated_port + 1000
        instance_id = f"bds-{uuid.uuid4().hex[:8]}"
        created_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

        record = {
            "id": instance_id,
            "display_name": clean_name,
            "world_name": clean_world,
            "seed": str(seed).strip(),
            "difficulty": clean_diff,
            "gamemode": clean_gm,
            "max_players": clean_max,
            "port": allocated_port,
            "ipv6_port": ipv6_port,
            "created_at": created_at,
        }

        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO instances (
                    id, display_name, world_name, seed, difficulty,
                    gamemode, max_players, port, ipv6_port, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record["id"],
                    record["display_name"],
                    record["world_name"],
                    record["seed"],
                    record["difficulty"],
                    record["gamemode"],
                    record["max_players"],
                    record["port"],
                    record["ipv6_port"],
                    record["created_at"],
                ),
            )
            conn.commit()

        self.write_server_properties(record)
        self.get_runtime(instance_id)
        self.append_log_sync(
            instance_id,
            f"\x1b[32m[SYSTEM]\x1b[0m Created instance '{clean_name}' "
            f"(World: {clean_world}, UDP Port: {allocated_port})",
        )
        return self.get_instance(instance_id)  # type: ignore[return-value]

    def update_instance(
        self,
        instance_id: str,
        display_name: str | None = None,
        world_name: str | None = None,
        seed: str | None = None,
        difficulty: str | None = None,
        gamemode: str | None = None,
        max_players: int | None = None,
    ) -> dict[str, Any]:
        inst = self.get_instance(instance_id)
        if not inst:
            raise ValueError(f"Instance '{instance_id}' not found")

        updated = {
            "id": instance_id,
            "display_name": (display_name.strip() if display_name else inst["display_name"]),
            "world_name": (
                re.sub(r'[<>:"/\\|?*]', "_", world_name.strip())
                if world_name
                else inst["world_name"]
            ),
            "seed": seed.strip() if seed is not None else inst["seed"],
            "difficulty": difficulty.lower() if difficulty else inst["difficulty"],
            "gamemode": gamemode.lower() if gamemode else inst["gamemode"],
            "max_players": int(max_players) if max_players is not None else inst["max_players"],
            "port": inst["port"],
            "ipv6_port": inst["ipv6_port"],
        }

        with self._get_conn() as conn:
            conn.execute(
                """
                UPDATE instances
                SET display_name = ?, world_name = ?, seed = ?,
                    difficulty = ?, gamemode = ?, max_players = ?
                WHERE id = ?
                """,
                (
                    updated["display_name"],
                    updated["world_name"],
                    updated["seed"],
                    updated["difficulty"],
                    updated["gamemode"],
                    updated["max_players"],
                    instance_id,
                ),
            )
            conn.commit()

        self.write_server_properties(updated)
        self.append_log_sync(
            instance_id,
            f"\x1b[36m[CONFIG]\x1b[0m Updated server.properties for '{updated['display_name']}'",
        )
        return self.get_instance(instance_id)  # type: ignore[return-value]

    async def delete_instance(self, instance_id: str) -> None:
        rt = self.runtimes.get(instance_id)
        if rt and rt.status != "offline":
            await self.kill_instance(instance_id)

        with self._get_conn() as conn:
            conn.execute("DELETE FROM instances WHERE id = ?", (instance_id,))
            conn.commit()

        self.runtimes.pop(instance_id, None)
        inst_dir = self.instance_dir(instance_id)
        if inst_dir.exists():
            # Safely remove any directory junctions first so shared bds_bin assets are never deleted
            for folder in ("behavior_packs", "resource_packs", "definitions", "config", "data", "structures"):
                junc = inst_dir / folder
                if junc.exists():
                    try:
                        os.rmdir(junc)
                    except OSError:
                        pass
            shutil.rmtree(inst_dir, ignore_errors=True)

    def get_runtime(self, instance_id: str) -> ManagedInstanceRuntime:
        if instance_id not in self.runtimes:
            self.runtimes[instance_id] = ManagedInstanceRuntime(instance_id)
        return self.runtimes[instance_id]

    def _enrich_instance(self, row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        data = dict(row)
        inst_id = data["id"]
        rt = self.get_runtime(inst_id)

        uptime_sec = 0.0
        cpu_pct = 0.0
        mem_mb = 0.0
        pid = None
        nethernet_udp_port: int | None = None

        if rt.process and rt.process.returncode is None and rt.status in ("online", "starting", "stopping"):
            pid = rt.process.pid
            if rt.started_at:
                uptime_sec = max(0.0, time.time() - rt.started_at)
            if rt.ps_proc is not None:
                try:
                    if rt.ps_proc.is_running():
                        raw_cpu = rt.ps_proc.cpu_percent(interval=None)
                        cpu_Count = psutil.cpu_count() or 1
                        cpu_pct = round(min(100.0, raw_cpu / cpu_Count if raw_cpu > 100 else raw_cpu), 1)
                        mem_mb = round(rt.ps_proc.memory_info().rss / (1024 * 1024), 1)
                        rt.cpu_percent = cpu_pct
                        rt.memory_mb = mem_mb
                        for conn in rt.ps_proc.net_connections(kind="udp"):
                            if conn.laddr and conn.laddr.port:
                                nethernet_udp_port = int(conn.laddr.port)
                                break
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        else:
            if rt.status != "offline":
                rt.status = "offline"
                rt.started_at = None
                rt.players_online = 0

        data.update(
            {
                "status": rt.status,
                "pid": pid,
                "uptime_seconds": round(uptime_sec, 1),
                "uptime_human": format_uptime(uptime_sec) if rt.status == "online" else "Offline",
                "cpu_percent": cpu_pct,
                "memory_mb": mem_mb,
                "players_online": rt.players_online if rt.status == "online" else 0,
                "engine_mode": rt.engine_mode,
                "nethernet_udp_port": nethernet_udp_port,
                "properties_path": str(self.instance_dir(inst_id) / "server.properties"),
            }
        )
        return data

    def list_instances(self) -> list[dict[str, Any]]:
        with self._get_conn() as conn:
            rows = conn.execute("SELECT * FROM instances ORDER BY port ASC").fetchall()
        return [self._enrich_instance(r) for r in rows]

    def get_instance(self, instance_id: str) -> dict[str, Any] | None:
        with self._get_conn() as conn:
            row = conn.execute("SELECT * FROM instances WHERE id = ?", (instance_id,)).fetchone()
        if not row:
            return None
        return self._enrich_instance(row)

    def get_console_logs(self, instance_id: str) -> list[dict[str, Any]]:
        rt = self.get_runtime(instance_id)
        return list(rt.logs)

    def append_log_sync(self, instance_id: str, line: str) -> dict[str, Any]:
        rt = self.get_runtime(instance_id)
        entry = {
            "timestamp": datetime.datetime.now().strftime("%H:%M:%S"),
            "line": line.rstrip("\r\n"),
        }
        rt.logs.append(entry)
        try:
            loop = asyncio.get_running_loop()
            if self.console_push_callback:
                loop.create_task(self.console_push_callback(instance_id, entry))
        except RuntimeError:
            pass
        return entry

    async def _append_log(self, instance_id: str, line: str) -> None:
        rt = self.get_runtime(instance_id)
        text = line.rstrip("\r\n")
        entry = {
            "timestamp": datetime.datetime.now().strftime("%H:%M:%S"),
            "line": text,
        }
        rt.logs.append(entry)

        # Detect save query completion output for hot backup synchronization
        if (
            "Data saved" in text
            or "Files are now ready to be copied" in text
            or "/db/MANIFEST" in text
            or "/level.dat:" in text
        ):
            rt.save_ready_event.set()

        # Track player count changes from BDS console logs
        if "Player connected:" in text or "Player session established" in text:
            rt.players_online += 1
        elif "Player disconnected:" in text and rt.players_online > 0:
            rt.players_online -= 1

        if self.console_push_callback:
            await self.console_push_callback(instance_id, entry)

        if "Server started." in text or "Accepting clients on" in text:
            await self._notify_state()

    def _resolve_bds_command(self, inst_dir: Path) -> tuple[list[str], str]:
        """
        Determine whether an official `bedrock_server` binary is available,
        or launch the built-in `app.bds_runtime` BDS-compatible engine.
        Ensures `bedrock_server.exe` and its essential asset folders are linked into
        `inst_dir` so each instance reads its own `server.properties` and `worlds/`.
        """
        exe_name = "bedrock_server.exe" if os.name == "nt" else "bedrock_server"
        local_exe = inst_dir / exe_name
        shared_exe = BDS_BIN_DIR / exe_name

        if shared_exe.exists() or local_exe.exists():
            if not local_exe.exists() and shared_exe.exists():
                try:
                    os.link(shared_exe, local_exe)
                except Exception:
                    shutil.copy2(shared_exe, local_exe)

            # Link or copy required BDS asset directories into inst_dir
            for folder in ("behavior_packs", "resource_packs", "definitions", "config", "data", "structures"):
                src_folder = BDS_BIN_DIR / folder
                dst_folder = inst_dir / folder
                if src_folder.exists() and not dst_folder.exists():
                    if os.name == "nt":
                        subprocess.run(
                            ["cmd", "/c", "mklink", "/J", str(dst_folder), str(src_folder)],
                            capture_output=True,
                        )
                    if not dst_folder.exists():
                        try:
                            os.symlink(src_folder, dst_folder, target_is_directory=True)
                        except Exception:
                            shutil.copytree(src_folder, dst_folder)

            for support_file in ("profanity_filter.wlist", "packetlimitconfig.json"):
                src_f = BDS_BIN_DIR / support_file
                dst_f = inst_dir / support_file
                if src_f.exists() and not dst_f.exists():
                    try:
                        shutil.copy2(src_f, dst_f)
                    except Exception:
                        pass

            # Remove any synthetic simulator LevelDB stub so official BDS creates a clean LevelDB
            worlds_root = inst_dir / "worlds"
            if worlds_root.exists():
                for manifest in worlds_root.rglob("MANIFEST-000001"):
                    try:
                        if manifest.read_bytes()[:24] == b"BDS-LEVELDB-MANIFEST-V1:":
                            shutil.rmtree(manifest.parent, ignore_errors=True)
                    except Exception:
                        pass

            return [str(local_exe)], "Official BDS Binary"

        return [sys.executable, "-u", "-m", "app.bds_runtime", str(inst_dir)], "Built-in BDS Engine"

    async def start_instance(self, instance_id: str) -> dict[str, Any]:
        """Start the BDS instance subprocess."""
        inst = self.get_instance(instance_id)
        if not inst:
            raise ValueError(f"Instance '{instance_id}' not found")

        rt = self.get_runtime(instance_id)
        async with rt.lock:
            if rt.process and rt.process.returncode is None:
                return self.get_instance(instance_id)  # type: ignore[return-value]

            inst_dir = self.instance_dir(instance_id)
            self.write_server_properties(inst)

            cmd, engine_mode = self._resolve_bds_command(inst_dir)
            rt.engine_mode = engine_mode
            rt.status = "starting"
            rt.players_online = 0

            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"
            existing_pp = env.get("PYTHONPATH", "")
            env["PYTHONPATH"] = f"{BASE_DIR}{os.pathsep}{existing_pp}" if existing_pp else str(BASE_DIR)
            if os.name != "nt":
                env["LD_LIBRARY_PATH"] = f"{inst_dir}:{BDS_BIN_DIR}"

            await self._append_log(
                instance_id,
                f"\x1b[36m[SUPERVISOR]\x1b[0m Starting '{inst['display_name']}' "
                f"on UDP :{inst['port']} ({engine_mode})...",
            )

            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                cwd=str(inst_dir),
                env=env,
            )
            rt.process = proc
            rt.started_at = time.time()
            rt.status = "online"
            try:
                rt.ps_proc = psutil.Process(proc.pid)
                rt.ps_proc.cpu_percent(interval=None)
            except Exception:
                rt.ps_proc = None

            rt.reader_task = asyncio.create_task(self._stdout_reader(instance_id, proc))
            rt.waiter_task = asyncio.create_task(self._process_waiter(instance_id, proc))

        if self.state_change_callback:
            await self.state_change_callback()

        return self.get_instance(instance_id)  # type: ignore[return-value]

    async def _stdout_reader(self, instance_id: str, proc: asyncio.subprocess.Process) -> None:
        if not proc.stdout:
            return
        try:
            while True:
                raw = await proc.stdout.readline()
                if not raw:
                    break
                line = raw.decode("utf-8", errors="replace")
                await self._append_log(instance_id, line)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.debug("Stdout reader error for %s: %s", instance_id, exc)

    async def _process_waiter(self, instance_id: str, proc: asyncio.subprocess.Process) -> None:
        try:
            code = await proc.wait()
            rt = self.get_runtime(instance_id)
            if rt.process is proc:
                rt.status = "offline"
                rt.started_at = None
                rt.players_online = 0
                rt.cpu_percent = 0.0
                rt.memory_mb = 0.0
                rt.ps_proc = None
            await self._append_log(
                instance_id,
                f"\x1b[90m[SUPERVISOR] Process exited with return code {code}.\x1b[0m",
            )
            if self.state_change_callback:
                await self.state_change_callback()
        except asyncio.CancelledError:
            pass

    async def send_command(self, instance_id: str, command: str, echo: bool = True) -> None:
        """Write an interactive console command to the active BDS process STDIN."""
        rt = self.get_runtime(instance_id)
        if not rt.process or rt.process.returncode is not None or not rt.process.stdin:
            raise RuntimeError(f"Instance '{instance_id}' is not currently running")

        clean = command.strip()
        if not clean:
            return

        if echo:
            await self._append_log(instance_id, f"\x1b[93m> {clean}\x1b[0m")

        rt.process.stdin.write((clean + "\n").encode("utf-8"))
        await rt.process.stdin.drain()

    async def stop_instance(self, instance_id: str, timeout: float = 8.0) -> dict[str, Any]:
        """Gracefully stop an instance by writing `stop` to STDIN."""
        rt = self.get_runtime(instance_id)
        async with rt.lock:
            proc = rt.process
            if not proc or proc.returncode is not None:
                rt.status = "offline"
                return self.get_instance(instance_id)  # type: ignore[return-value]

            rt.status = "stopping"
            await self._append_log(
                instance_id,
                "\x1b[33m[SUPERVISOR]\x1b[0m Sending graceful 'stop' command to STDIN...",
            )
            try:
                if proc.stdin:
                    proc.stdin.write(b"stop\n")
                    await proc.stdin.drain()
                await asyncio.wait_for(proc.wait(), timeout=timeout)
            except (asyncio.TimeoutError, Exception):
                await self._append_log(
                    instance_id,
                    "\x1b[31m[SUPERVISOR]\x1b[0m Graceful stop timed out; terminating process.",
                )
                try:
                    proc.terminate()
                    await asyncio.wait_for(proc.wait(), timeout=3.0)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass

            rt.status = "offline"
            rt.started_at = None
            rt.players_online = 0
            rt.ps_proc = None

        if self.state_change_callback:
            await self.state_change_callback()

        return self.get_instance(instance_id)  # type: ignore[return-value]

    async def restart_instance(self, instance_id: str) -> dict[str, Any]:
        """Gracefully restart an instance (`stop` via STDIN -> wait -> start)."""
        await self._append_log(
            instance_id,
            "\x1b[36m[SUPERVISOR]\x1b[0m Initiating graceful restart...",
        )
        await self.stop_instance(instance_id)
        await asyncio.sleep(0.25)
        return await self.start_instance(instance_id)

    async def kill_instance(self, instance_id: str) -> dict[str, Any]:
        """Force kill an instance process immediately."""
        rt = self.get_runtime(instance_id)
        async with rt.lock:
            proc = rt.process
            if proc and proc.returncode is None:
                await self._append_log(
                    instance_id,
                    "\x1b[91m[SUPERVISOR] Force killing BDS process (SIGKILL)...\x1b[0m",
                )
                try:
                    if rt.ps_proc:
                        for child in rt.ps_proc.children(recursive=True):
                            child.kill()
                    proc.kill()
                    await asyncio.wait_for(proc.wait(), timeout=3.0)
                except Exception:
                    pass
            rt.status = "offline"
            rt.started_at = None
            rt.players_online = 0
            rt.ps_proc = None

        if self.state_change_callback:
            await self.state_change_callback()

        return self.get_instance(instance_id)  # type: ignore[return-value]

    async def stop_all(self) -> None:
        """Stop all running instances on server shutdown."""
        running_ids = [
            inst_id
            for inst_id, rt in self.runtimes.items()
            if rt.process and rt.process.returncode is None
        ]
        if running_ids:
            await asyncio.gather(*(self.stop_instance(i, timeout=4.0) for i in running_ids))

    # -------------------------------------------------------------------------
    # World & Backup Automation (`save hold` -> `save query` -> zip -> `save resume`)
    # -------------------------------------------------------------------------

    async def create_backup(self, instance_id: str) -> dict[str, Any]:
        """
        Execute automated world backup:
        If instance is online:
          1. Dispatch `save hold` via BDS STDIN
          2. Poll `save query` via BDS STDIN until world files are ready
          3. Zip world directory into `/backups` with timestamp
          4. Always dispatch `save resume` via BDS STDIN
        If instance is offline:
          Zip world directory directly into `/backups`.
        """
        inst = self.get_instance(instance_id)
        if not inst:
            raise ValueError(f"Instance '{instance_id}' not found")

        world_name = inst["world_name"]
        w_dir = self.world_dir(instance_id, world_name)
        ensure_world_files(self.instance_dir(instance_id), world_name, inst.get("seed", ""))

        rt = self.get_runtime(instance_id)
        is_online = rt.process is not None and rt.process.returncode is None and rt.status == "online"

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_world = re.sub(r"[^a-zA-Z0-9_-]", "_", world_name)
        backup_filename = f"{instance_id}_{safe_world}_{timestamp}.zip"
        backup_path = BACKUPS_DIR / backup_filename

        if is_online:
            rt.save_ready_event.clear()
            await self._append_log(
                instance_id,
                "\x1b[36m[BACKUP]\x1b[0m Starting live world backup: dispatching 'save hold'...",
            )
            try:
                await self.send_command(instance_id, "save hold", echo=True)
                # Poll `save query` until BDS confirms files are ready
                for _ in range(12):
                    await asyncio.sleep(0.25)
                    await self.send_command(instance_id, "save query", echo=True)
                    try:
                        await asyncio.wait_for(rt.save_ready_event.wait(), timeout=0.45)
                        break
                    except asyncio.TimeoutError:
                        continue

                await self._append_log(
                    instance_id,
                    f"\x1b[36m[BACKUP]\x1b[0m Archiving world '{world_name}' -> backups/{backup_filename}...",
                )
                await asyncio.to_thread(self._zip_world_directory, w_dir, backup_path, inst)
            finally:
                try:
                    await self.send_command(instance_id, "save resume", echo=True)
                    await self._append_log(
                        instance_id,
                        f"\x1b[32m[BACKUP]\x1b[0m Backup completed ({backup_filename}) and 'save resume' executed.",
                    )
                except Exception:
                    pass
        else:
            await self._append_log(
                instance_id,
                f"\x1b[36m[BACKUP]\x1b[0m Creating offline world backup -> backups/{backup_filename}...",
            )
            await asyncio.to_thread(self._zip_world_directory, w_dir, backup_path, inst)
            await self._append_log(
                instance_id,
                f"\x1b[32m[BACKUP]\x1b[0m Offline backup saved: {backup_filename}",
            )

        return self._inspect_backup_file(backup_path)

    def _zip_world_directory(self, world_dir: Path, backup_path: Path, inst: dict[str, Any]) -> None:
        with zipfile.ZipFile(backup_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for file_path in world_dir.rglob("*"):
                if file_path.is_file():
                    arcname = file_path.relative_to(world_dir).as_posix()
                    zf.write(file_path, arcname)
            meta = {
                "instance_id": inst["id"],
                "display_name": inst["display_name"],
                "world_name": inst["world_name"],
                "backed_up_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            }
            zf.writestr("backup_meta.json", json.dumps(meta, indent=2))

    def _inspect_backup_file(self, path: Path) -> dict[str, Any]:
        stat = path.stat()
        meta: dict[str, Any] = {}
        try:
            with zipfile.ZipFile(path, "r") as zf:
                if "backup_meta.json" in zf.namelist():
                    meta = json.loads(zf.read("backup_meta.json").decode("utf-8"))
        except Exception:
            pass

        name_parts = path.stem.split("_")
        inferred_inst = meta.get("instance_id") or (name_parts[0] if name_parts else "")
        inferred_world = meta.get("world_name") or (
            "_".join(name_parts[1:-2]) if len(name_parts) > 3 else path.stem
        )

        return {
            "filename": path.name,
            "instance_id": inferred_inst,
            "display_name": meta.get("display_name", inferred_inst),
            "world_name": inferred_world,
            "size_bytes": stat.st_size,
            "size_human": format_bytes(stat.st_size),
            "created_at": datetime.datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            "timestamp": stat.st_mtime,
        }

    def list_backups(self, instance_id: str | None = None) -> list[dict[str, Any]]:
        backups: list[dict[str, Any]] = []
        for p in sorted(BACKUPS_DIR.glob("*.zip"), key=lambda x: x.stat().st_mtime, reverse=True):
            info = self._inspect_backup_file(p)
            if instance_id is None or info["instance_id"] == instance_id:
                backups.append(info)
        return backups

    def delete_backup(self, filename: str) -> None:
        safe_name = Path(filename).name
        target = BACKUPS_DIR / safe_name
        if not target.exists():
            raise FileNotFoundError(f"Backup '{safe_name}' not found")
        target.unlink()

    async def restore_backup(self, instance_id: str, filename: str) -> dict[str, Any]:
        """Restore a backup archive into the specified instance's world folder."""
        inst = self.get_instance(instance_id)
        if not inst:
            raise ValueError(f"Instance '{instance_id}' not found")

        safe_name = Path(filename).name
        backup_path = BACKUPS_DIR / safe_name
        if not backup_path.exists():
            raise FileNotFoundError(f"Backup '{safe_name}' not found")

        was_online = inst["status"] == "online"
        if was_online:
            await self._append_log(
                instance_id,
                f"\x1b[33m[RESTORE]\x1b[0m Stopping instance before restoring '{safe_name}'...",
            )
            await self.stop_instance(instance_id)

        w_dir = self.world_dir(instance_id, inst["world_name"])
        await asyncio.to_thread(self._extract_archive_to_world, backup_path.read_bytes(), w_dir, inst["world_name"])

        await self._append_log(
            instance_id,
            f"\x1b[32m[RESTORE]\x1b[0m Restored world '{inst['world_name']}' from '{safe_name}'.",
        )

        if was_online:
            await self.start_instance(instance_id)

        return self.get_instance(instance_id)  # type: ignore[return-value]

    async def import_world_archive(
        self,
        instance_id: str,
        upload_filename: str,
        archive_bytes: bytes,
    ) -> dict[str, Any]:
        """Import a `.mcworld` or `.zip` archive directly into an instance's active world."""
        inst = self.get_instance(instance_id)
        if not inst:
            raise ValueError(f"Instance '{instance_id}' not found")

        was_online = inst["status"] == "online"
        if was_online:
            await self._append_log(
                instance_id,
                f"\x1b[33m[IMPORT]\x1b[0m Stopping server to import world '{upload_filename}'...",
            )
            await self.stop_instance(instance_id)

        w_dir = self.world_dir(instance_id, inst["world_name"])
        imported_name = await asyncio.to_thread(
            self._extract_archive_to_world,
            archive_bytes,
            w_dir,
            inst["world_name"],
        )

        # Save a copy in /backups as an import snapshot
        ts_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_up = re.sub(r"[^a-zA-Z0-9_-]", "_", Path(upload_filename).stem)
        snapshot_path = BACKUPS_DIR / f"{instance_id}_import_{safe_up}_{ts_str}.zip"
        await asyncio.to_thread(self._zip_world_directory, w_dir, snapshot_path, inst)

        await self._append_log(
            instance_id,
            f"\x1b[32m[IMPORT]\x1b[0m Successfully imported '{upload_filename}' "
            f"(Level: {imported_name}) into '{inst['world_name']}'.",
        )

        if was_online:
            await self.start_instance(instance_id)

        return self.get_instance(instance_id)  # type: ignore[return-value]

    def _extract_archive_to_world(self, archive_bytes: bytes, target_world_dir: Path, fallback_level_name: str) -> str:
        """
        Safely extract a `.mcworld` or `.zip` archive into `target_world_dir`.
        Handles archives where files are either at root or nested inside a top-level folder.
        """
        with zipfile.ZipFile(io.BytesIO(archive_bytes), "r") as zf:
            names = [n for n in zf.namelist() if not n.endswith("/")]
            if not names:
                raise ValueError("Uploaded archive is empty")

            # Determine if level.dat or levelname.txt is nested inside a subfolder
            prefix = ""
            for candidate in names:
                base = Path(candidate).name.lower()
                if base in ("level.dat", "levelname.txt"):
                    parent = Path(candidate).parent.as_posix()
                    prefix = "" if parent == "." else parent + "/"
                    break

            if target_world_dir.exists():
                shutil.rmtree(target_world_dir, ignore_errors=True)
            target_world_dir.mkdir(parents=True, exist_ok=True)

            target_root = target_world_dir.resolve()
            for member in zf.infolist():
                if member.is_dir():
                    continue
                member_name = member.filename
                if member_name == "backup_meta.json":
                    continue
                if prefix and member_name.startswith(prefix):
                    rel_name = member_name[len(prefix) :]
                else:
                    rel_name = member_name
                if not rel_name:
                    continue
                dest = (target_world_dir / rel_name).resolve()
                # Prevent Zip Slip path traversal
                if not str(dest).startswith(str(target_root)):
                    continue
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(zf.read(member))

        # Ensure standard BDS world files exist even if a minimal zip was uploaded
        ensure_world_files(target_world_dir.parent.parent, fallback_level_name, "")
        levelname_file = target_world_dir / "levelname.txt"
        if levelname_file.exists():
            return levelname_file.read_text(encoding="utf-8", errors="replace").strip() or fallback_level_name
        levelname_file.write_text(fallback_level_name, encoding="utf-8")
        return fallback_level_name

