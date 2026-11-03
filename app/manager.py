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
import ctypes
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
from app.bootstrap import (
    clean_simulator_world_stubs,
    discover_pid_nethernet_port,
    ensure_bds_binary,
    ensure_vc_redist,
    sync_vc_dlls_to_directory,
)
from app.world_options import (
    DEFAULT_WORLD_OPTIONS,
    apply_options_to_level_dat,
    build_live_gamerule_commands,
    build_server_properties,
    build_starter_chest_commands,
    build_starter_house_commands,
    build_starter_kit_commands,
    normalize_world_options,
    read_options_from_level_dat,
)

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


_KERNEL32 = ctypes.WinDLL("kernel32", use_last_error=True) if os.name == "nt" else None
_PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
try:
    _FREQ_INFO = psutil.cpu_freq()
    _HOST_CPU_FREQ_HZ = float((_FREQ_INFO.max or _FREQ_INFO.current or 2800.0) if _FREQ_INFO else 2800.0) * 1_000_000.0
except Exception:
    _HOST_CPU_FREQ_HZ = 2_800_000_000.0


def get_process_cpu_cycles(pid: int) -> int:
    """
    Query hardware CPU clock cycles consumed by `pid` on Windows via `QueryProcessCycleTime`.
    Unlike `GetProcessTimes` (which only samples on 15.6ms clock interrupts and misses BDS's
    short 20 TPS tick wakeups), `QueryProcessCycleTime` captures exact RDTSC thread cycles.
    """
    if _KERNEL32 is None or pid <= 0:
        return 0
    handle = _KERNEL32.OpenProcess(_PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid))
    if not handle:
        return 0
    try:
        cycles = ctypes.c_ulonglong(0)
        if _KERNEL32.QueryProcessCycleTime(handle, ctypes.byref(cycles)):
            return int(cycles.value)
        return 0
    except Exception:
        return 0
    finally:
        _KERNEL32.CloseHandle(handle)


_RE_PLAYER_CONNECTED = re.compile(r"Player connected:\s*(.+?),\s*xuid:\s*([0-9a-fA-F-]+)")
_RE_PLAYER_SPAWNED = re.compile(r"Player Spawned:\s*(.+?)\s+xuid:\s*([0-9a-fA-F-]+)")
_RE_PLAYER_DISCONNECTED = re.compile(r"Player disconnected:\s*(.+?),\s*xuid:\s*([0-9a-fA-F-]+)")
_RE_PLAYERS_LIST_HDR = re.compile(r"There are\s+(\d+)\s*/\s*(\d+)\s+players online:")
_RE_LOG_PREFIX = re.compile(r"^\[[^\]]+\]\s*")


class ManagedInstanceRuntime:
    """Tracks live subprocess, console ring buffer, connected players, and telemetry for one BDS instance."""

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
        self.players: dict[str, dict[str, Any]] = {}
        self.xuid_to_name: dict[str, str] = {}
        self.cpu_percent: float = 0.0
        self.memory_mb: float = 0.0
        self.memory_percent: float = 0.0
        self.nethernet_udp_port: int | None = None
        self._last_metrics_at: float = 0.0
        self._last_sample_wall: float = 0.0
        self._last_cycles: int = 0
        self._last_cpu_time: float = 0.0
        self._silent_list_until: float = 0.0
        self._expecting_list_names: bool = False
        self._eat_empty_after_list: bool = False
        self.lock = asyncio.Lock()

    def reset_runtime_metrics(self) -> None:
        self.players_online = 0
        self.players.clear()
        self.xuid_to_name.clear()
        self.cpu_percent = 0.0
        self.memory_mb = 0.0
        self.memory_percent = 0.0
        self.nethernet_udp_port = None
        self._last_metrics_at = 0.0
        self._last_sample_wall = 0.0
        self._last_cycles = 0
        self._last_cpu_time = 0.0
        self._silent_list_until = 0.0
        self._expecting_list_names = False
        self._eat_empty_after_list = False


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
                    created_at TEXT NOT NULL,
                    options_json TEXT NOT NULL DEFAULT '{}'
                )
                """
            )
            # Migrate existing instances table if options_json column does not exist yet
            cols = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(instances)").fetchall()
            }
            if "options_json" not in cols:
                conn.execute(
                    "ALTER TABLE instances ADD COLUMN options_json TEXT NOT NULL DEFAULT '{}'"
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
        """Write `server.properties` and synchronize `worlds/<world_name>/level.dat` options."""
        inst_dir = self.instance_dir(instance["id"])
        inst_dir.mkdir(parents=True, exist_ok=True)
        props_path = inst_dir / "server.properties"

        opts = normalize_world_options(instance.get("options"))
        instance["options"] = opts
        content = build_server_properties(instance, opts)
        props_path.write_text(content, encoding="utf-8")

        for json_file in ("permissions.json", "allowlist.json"):
            jf = inst_dir / json_file
            if not jf.exists():
                jf.write_text("[]", encoding="utf-8")

        w_dir = inst_dir / "worlds" / instance["world_name"]
        w_dir.mkdir(parents=True, exist_ok=True)
        (w_dir / "levelname.txt").write_text(instance["world_name"], encoding="utf-8")
        try:
            apply_options_to_level_dat(w_dir, instance, opts)
        except Exception as exc:
            logger.warning("Failed to apply level.dat options for %s: %s", instance["id"], exc)
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
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create a new BDS instance, allocate its UDP port, and write `server.properties` & `level.dat`."""
        clean_name = display_name.strip() or "Bedrock Server"
        clean_world = re.sub(r'[<>:"/\\|?*]', "_", world_name.strip() or "BedrockLevel")
        clean_opts = normalize_world_options(options)
        clean_diff = "hard" if clean_opts.get("hardcore") else difficulty.lower().strip()
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
            "options": clean_opts,
            "options_json": json.dumps(clean_opts),
        }

        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO instances (
                    id, display_name, world_name, seed, difficulty,
                    gamemode, max_players, port, ipv6_port, created_at, options_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    record["options_json"],
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
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        inst = self.get_instance(instance_id)
        if not inst:
            raise ValueError(f"Instance '{instance_id}' not found")

        merged_opts = normalize_world_options(options, inst.get("options"))
        next_diff = difficulty.lower() if difficulty else inst["difficulty"]
        if merged_opts.get("hardcore"):
            next_diff = "hard"

        updated = {
            "id": instance_id,
            "display_name": (display_name.strip() if display_name else inst["display_name"]),
            "world_name": (
                re.sub(r'[<>:"/\\|?*]', "_", world_name.strip())
                if world_name
                else inst["world_name"]
            ),
            "seed": seed.strip() if seed is not None else inst["seed"],
            "difficulty": next_diff,
            "gamemode": gamemode.lower() if gamemode else inst["gamemode"],
            "max_players": int(max_players) if max_players is not None else inst["max_players"],
            "port": inst["port"],
            "ipv6_port": inst["ipv6_port"],
            "options": merged_opts,
            "options_json": json.dumps(merged_opts),
        }

        with self._get_conn() as conn:
            conn.execute(
                """
                UPDATE instances
                SET display_name = ?, world_name = ?, seed = ?,
                    difficulty = ?, gamemode = ?, max_players = ?, options_json = ?
                WHERE id = ?
                """,
                (
                    updated["display_name"],
                    updated["world_name"],
                    updated["seed"],
                    updated["difficulty"],
                    updated["gamemode"],
                    updated["max_players"],
                    updated["options_json"],
                    instance_id,
                ),
            )
            conn.commit()

        # Write server.properties; only rewrite level.dat directly on disk if instance is offline
        # (when online, BDS holds level.dat open and overwrites it on shutdown, so we send live STDIN gamerules!)
        rt = self.get_runtime(instance_id)
        if rt.status == "offline":
            self.write_server_properties(updated)
        else:
            inst_dir = self.instance_dir(instance_id)
            inst_dir.mkdir(parents=True, exist_ok=True)
            (inst_dir / "server.properties").write_text(
                build_server_properties(updated, merged_opts), encoding="utf-8"
            )

        self.append_log_sync(
            instance_id,
            f"\x1b[36m[CONFIG]\x1b[0m Updated server & world options for '{updated['display_name']}'",
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

    async def _notify_state(self) -> None:
        if self.state_change_callback:
            try:
                await self.state_change_callback()
            except Exception as exc:
                logger.debug("State change callback error: %s", exc)

    def _sample_runtime_telemetry(self, rt: ManagedInstanceRuntime, pid: int) -> None:
        """
        Sample per-process CPU % (combining hardware cycle counters via `QueryProcessCycleTime`
        and kernel `cpu_times`), memory RSS (MB), and bound NetherNet UDP port at most once
        every 0.85 seconds so back-to-back API/WebSocket calls never reset CPU deltas to 0.0%.
        """
        now_wall = time.perf_counter()
        if now_wall - rt._last_metrics_at < 0.85 and rt._last_metrics_at > 0.0:
            return

        dt = max(0.05, now_wall - rt._last_sample_wall) if rt._last_sample_wall > 0.0 else 0.0
        rt._last_metrics_at = now_wall

        if rt.ps_proc is not None:
            try:
                if rt.ps_proc.is_running():
                    cycles_now = get_process_cpu_cycles(pid)
                    ct = rt.ps_proc.cpu_times()
                    cpu_time_now = float(ct.user + ct.system)

                    if dt > 0.0:
                        d_cycles = max(0, cycles_now - rt._last_cycles) if rt._last_cycles > 0 else 0
                        cycle_pct = (d_cycles / (_HOST_CPU_FREQ_HZ * dt)) * 100.0

                        d_cpu = max(0.0, cpu_time_now - rt._last_cpu_time) if rt._last_cpu_time > 0.0 else 0.0
                        ps_pct = (d_cpu / dt) * 100.0

                        raw_pct = max(cycle_pct, ps_pct)
                        smoothed = (0.65 * raw_pct + 0.35 * rt.cpu_percent) if rt.cpu_percent > 0.0 else raw_pct
                        rt.cpu_percent = round(min(100.0, max(0.1 if raw_pct > 0.02 else 0.0, smoothed)), 1)

                    rt._last_cycles = cycles_now
                    rt._last_cpu_time = cpu_time_now
                    rt._last_sample_wall = now_wall

                    mem_info = rt.ps_proc.memory_info()
                    rt.memory_mb = round(mem_info.rss / (1024 * 1024), 1)
                    try:
                        rt.memory_percent = round(rt.ps_proc.memory_percent(), 1)
                    except Exception:
                        pass

                    found_nn_port: int | None = None
                    for conn in rt.ps_proc.net_connections(kind="udp"):
                        if conn.laddr and conn.laddr.port:
                            p = int(conn.laddr.port)
                            if str(conn.laddr.ip) in ("::", "0.0.0.0", ""):
                                found_nn_port = p
                                break
                            if found_nn_port is None:
                                found_nn_port = p
                    if found_nn_port is not None:
                        rt.nethernet_udp_port = found_nn_port
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        if rt.nethernet_udp_port is None and pid > 0:
            rt.nethernet_udp_port = discover_pid_nethernet_port(int(pid))

    def _enrich_instance(self, row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        data = dict(row)
        inst_id = data["id"]
        rt = self.get_runtime(inst_id)

        uptime_sec = 0.0
        cpu_pct = 0.0
        mem_mb = 0.0
        mem_pct = 0.0
        pid = None
        nethernet_udp_port: int | None = None

        if rt.process and rt.process.returncode is None and rt.status in ("online", "starting", "stopping"):
            pid = rt.process.pid
            if rt.started_at:
                uptime_sec = max(0.0, time.time() - rt.started_at)
            self._sample_runtime_telemetry(rt, int(pid))
            cpu_pct = rt.cpu_percent
            mem_mb = rt.memory_mb
            mem_pct = rt.memory_percent
            nethernet_udp_port = rt.nethernet_udp_port
        else:
            if rt.status != "offline":
                rt.status = "offline"
                rt.started_at = None
                rt.reset_runtime_metrics()

        raw_opts_json = data.get("options_json") or "{}"
        try:
            parsed_opts = json.loads(raw_opts_json) if isinstance(raw_opts_json, str) else {}
        except Exception:
            parsed_opts = {}

        if not parsed_opts:
            w_dir = self.world_dir(inst_id, str(data.get("world_name", "BedrockLevel")))
            opts = read_options_from_level_dat(w_dir)
        else:
            opts = normalize_world_options(parsed_opts)

        is_active = rt.status in ("online", "starting")
        players_list = list(rt.players.keys()) if is_active else []
        player_details = list(rt.players.values()) if is_active else []
        players_online = max(rt.players_online, len(players_list)) if is_active else 0

        data.update(
            {
                "status": rt.status,
                "pid": pid,
                "uptime_seconds": round(uptime_sec, 1),
                "uptime_human": format_uptime(uptime_sec) if is_active else "Offline",
                "cpu_percent": cpu_pct,
                "memory_mb": mem_mb,
                "memory_percent": mem_pct,
                "players_online": players_online,
                "players_list": players_list,
                "player_details": player_details,
                "engine_mode": rt.engine_mode,
                "nethernet_udp_port": nethernet_udp_port,
                "properties_path": str(self.instance_dir(inst_id) / "server.properties"),
                "options": opts,
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

    async def poll_online_players(self) -> None:
        """
        Silently send `list` to all running BDS instances so `players_online` and `players_list`
        are continuously verified against the server's authoritative player table.
        """
        now = time.time()
        for inst_id, rt in list(self.runtimes.items()):
            if (
                rt.status == "online"
                and rt.process is not None
                and rt.process.returncode is None
                and rt.process.stdin is not None
                and rt.started_at is not None
                and (now - rt.started_at) > 4.0
            ):
                try:
                    rt._silent_list_until = time.time() + 1.8
                    rt.process.stdin.write(b"list\n")
                    await rt.process.stdin.drain()
                except Exception:
                    pass

    async def _append_log(self, instance_id: str, line: str) -> None:
        rt = self.get_runtime(instance_id)
        text = line.rstrip("\r\n")
        now_ts = datetime.datetime.now().strftime("%H:%M:%S")
        is_silent_list = time.time() < rt._silent_list_until

        # 1. Handle the line immediately following `There are X/Y players online:` when X > 0
        if rt._expecting_list_names:
            rt._expecting_list_names = False
            stripped = _RE_LOG_PREFIX.sub("", text).strip()
            # Ensure this line is the comma-separated player roster and not an unrelated log line
            if stripped and not any(
                tag in stripped
                for tag in ("[INFO]", "[WARN]", "[ERROR]", "Server ", "Player ", "Saving ", "Data saved")
            ):
                names = [n.strip() for n in stripped.split(",") if n.strip()]
                new_players: dict[str, dict[str, Any]] = {}
                for n in names:
                    existing = rt.players.get(n)
                    if existing:
                        new_players[n] = existing
                    else:
                        new_players[n] = {
                            "name": n,
                            "xuid": "",
                            "joined_at": now_ts,
                            "status": "online",
                        }
                rt.players = new_players
                rt.players_online = len(rt.players)
                await self._notify_state()
                if is_silent_list:
                    return

        # 2. Handle empty trailing line after `There are 0/Y players online:` during silent poll
        if rt._eat_empty_after_list:
            rt._eat_empty_after_list = False
            if not text.strip() and is_silent_list:
                return

        # 3. Handle `There are X/Y players online:` output from `list`
        m_list = _RE_PLAYERS_LIST_HDR.search(text)
        if m_list:
            online_cnt = int(m_list.group(1))
            prev_cnt = rt.players_online
            if online_cnt == 0:
                rt.players.clear()
                rt.xuid_to_name.clear()
                rt.players_online = 0
                rt._expecting_list_names = False
                rt._eat_empty_after_list = is_silent_list
                if prev_cnt != 0:
                    await self._notify_state()
            else:
                rt.players_online = online_cnt
                rt._expecting_list_names = True
                if prev_cnt != online_cnt:
                    await self._notify_state()
            if is_silent_list:
                return

        entry = {
            "timestamp": now_ts,
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

        # Track player connect / spawn / disconnect with exact player names and XUIDs
        player_state_changed = False
        m_conn = _RE_PLAYER_CONNECTED.search(text)
        if m_conn:
            p_name = m_conn.group(1).strip()
            p_xuid = m_conn.group(2).strip()
            existing = rt.players.get(p_name)
            rt.players[p_name] = {
                "name": p_name,
                "xuid": p_xuid,
                "joined_at": existing["joined_at"] if existing else now_ts,
                "status": "online",
            }
            if p_xuid:
                rt.xuid_to_name[p_xuid] = p_name
            rt.players_online = len(rt.players)
            player_state_changed = True
            asyncio.create_task(self._on_player_join_provision(instance_id))
        else:
            m_spawn = _RE_PLAYER_SPAWNED.search(text)
            if m_spawn:
                p_name = m_spawn.group(1).strip()
                p_xuid = m_spawn.group(2).strip()
                existing = rt.players.get(p_name)
                rt.players[p_name] = {
                    "name": p_name,
                    "xuid": p_xuid or (existing["xuid"] if existing else ""),
                    "joined_at": existing["joined_at"] if existing else now_ts,
                    "status": "online",
                }
                if p_xuid:
                    rt.xuid_to_name[p_xuid] = p_name
                rt.players_online = len(rt.players)
                player_state_changed = True
                asyncio.create_task(self._on_player_join_provision(instance_id, immediate=True))
            else:
                m_disc = _RE_PLAYER_DISCONNECTED.search(text)
                if m_disc:
                    p_name = m_disc.group(1).strip()
                    p_xuid = m_disc.group(2).strip()
                    rt.players.pop(p_name, None)
                    if p_xuid and p_xuid in rt.xuid_to_name:
                        mapped_name = rt.xuid_to_name.pop(p_xuid)
                        rt.players.pop(mapped_name, None)
                    rt.players_online = len(rt.players)
                    player_state_changed = True
                elif "Player disconnected:" in text and rt.players_online > 0:
                    rt.players_online = max(0, rt.players_online - 1)
                    player_state_changed = True

        # Confirm starter house / supply chest placement once BDS reports blocks filled or placed
        if ("blocks filled" in text or "Block placed" in text) and getattr(
            rt, "_pending_structure_flag_path", None
        ):
            try:
                flag_p: Path = getattr(rt, "_pending_structure_flag_path")
                flag_p.parent.mkdir(parents=True, exist_ok=True)
                flag_p.write_text("1", encoding="utf-8")
                setattr(rt, "_pending_structure_flag_path", None)
            except Exception:
                pass

        if self.console_push_callback:
            try:
                await self.console_push_callback(instance_id, entry)
            except Exception:
                pass

        if player_state_changed:
            await self._notify_state()

        if "Server started." in text or "Accepting clients on" in text:
            if rt.status == "starting":
                rt.status = "online"
            if "Server started." in text:
                asyncio.create_task(self.apply_live_options(instance_id))
            await self._notify_state()

    async def _on_player_join_provision(self, instance_id: str, immediate: bool = False) -> None:
        """
        Automatically provision configured Starter Inventory Kits, Stocked Supply Chests,
        and Co-Op Starter Houses when players spawn into the world.
        """
        delays = (0.8,) if immediate else (2.5, 6.5, 12.0)
        for d in delays:
            await asyncio.sleep(d)
            inst = self.get_instance(instance_id)
            if not inst:
                return
            rt = self.get_runtime(instance_id)
            if not rt.process or rt.process.returncode is not None or not rt.process.stdin:
                return

            opts = normalize_world_options(inst.get("options"))
            kit_tier = str(opts.get("starter_kit", "none"))
            chest_mode = str(opts.get("starter_chest_loadout", "none"))
            house_type = str(opts.get("starter_house", "none"))

            cmds: list[str] = []

            # 1. Auto-build Starter House and/or Stocked Supply Chests once per world at spawn
            if house_type != "none" or chest_mode != "none":
                w_dir = self.world_dir(instance_id, inst["world_name"])
                flag_file = w_dir / "bn_structure_built.flag"
                if not flag_file.exists():
                    setattr(rt, "_pending_structure_flag_path", flag_file)
                    if house_type != "none":
                        cmds.extend(build_starter_house_commands(house_type, at_selector="@a[c=1]"))
                    if chest_mode != "none":
                        cmds.extend(build_starter_chest_commands(chest_mode, at_selector="@a[c=1]"))

            # 2. Auto-give Starter Inventory Kit to any player without `bn_starter_given` tag
            if kit_tier != "none":
                cmds.extend(
                    build_starter_kit_commands(
                        kit_tier,
                        selector="@a[tag=!bn_starter_given]",
                        tag_after=True,
                    )
                )

            if cmds:
                if not opts.get("allow_cheats", True):
                    cmds.append("changesetting allow-cheats false")
                try:
                    for c in cmds:
                        rt.process.stdin.write((c + "\n").encode("utf-8"))
                    await rt.process.stdin.drain()
                except Exception:
                    return

    async def trigger_starter_action(
        self,
        instance_id: str,
        action_type: str,
        preset: str = "",
    ) -> dict[str, Any]:
        """
        Immediately dispatch a Starter Kit, Stocked Supply Chest, or Co-Op House build
        to all online players on a running server via STDIN.
        """
        inst = self.get_instance(instance_id)
        if not inst:
            raise ValueError(f"Instance '{instance_id}' not found")
        rt = self.get_runtime(instance_id)
        if not rt.process or rt.process.returncode is not None or not rt.process.stdin:
            raise RuntimeError("Server instance must be ONLINE to spawn starter kits or structures.")

        opts = normalize_world_options(inst.get("options"))
        cmds: list[str] = []
        label = ""

        if action_type == "kit":
            tier = preset or str(opts.get("starter_kit", "homestead_builder"))
            if tier == "none":
                tier = "homestead_builder"
            cmds = build_starter_kit_commands(tier, selector="@a", tag_after=False)
            label = f"Starter Kit ({tier})"
        elif action_type == "chest":
            mode = preset or str(opts.get("starter_chest_loadout", "house_builder_chest"))
            if mode == "none":
                mode = "house_builder_chest"
            cmds = build_starter_chest_commands(mode, at_selector="@a[c=1]")
            label = f"Supply Chest ({mode})"
        elif action_type == "house":
            kind = preset or str(opts.get("starter_house", "coop_cabin"))
            if kind == "none":
                kind = "coop_cabin"
            cmds = build_starter_house_commands(kind, at_selector="@a[c=1]")
            label = f"Starter Structure ({kind})"
        else:
            raise ValueError(f"Unknown starter action_type: {action_type}")

        if not opts.get("allow_cheats", True):
            cmds.append("changesetting allow-cheats false")

        for c in cmds:
            rt.process.stdin.write((c + "\n").encode("utf-8"))
        await rt.process.stdin.drain()

        await self._append_log(
            instance_id,
            f"\x1b[32m[HOMESTEAD]\x1b[0m Dispatched {label} ({len(cmds)} commands) to online players.",
        )
        return {"ok": True, "action_type": action_type, "preset": preset, "commands_sent": len(cmds)}

    async def apply_live_options(self, instance_id: str) -> None:
        """Send configured `gamerule` and `difficulty` commands to a running BDS instance via STDIN."""
        inst = self.get_instance(instance_id)
        if not inst:
            return
        rt = self.get_runtime(instance_id)
        if not rt.process or rt.process.returncode is not None or not rt.process.stdin:
            return
        cmds = build_live_gamerule_commands(inst, inst.get("options"))
        try:
            for c in cmds:
                rt.process.stdin.write((c + "\n").encode("utf-8"))
            await rt.process.stdin.drain()
        except Exception as exc:
            logger.debug("Could not dispatch live gamerules to %s: %s", instance_id, exc)

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

        if not shared_exe.exists() and not local_exe.exists():
            ensure_vc_redist(auto_install=True)
            ensure_bds_binary(auto_download=True)

        if shared_exe.exists() or local_exe.exists():
            sync_vc_dlls_to_directory(BDS_BIN_DIR)
            sync_vc_dlls_to_directory(inst_dir)
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

            # Remove any synthetic simulator LevelDB or JSON level.dat stub so official BDS creates a clean LevelDB
            clean_simulator_world_stubs()

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
            rt.reset_runtime_metrics()

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
                rt._last_cycles = get_process_cpu_cycles(proc.pid)
                ct = rt.ps_proc.cpu_times()
                rt._last_cpu_time = float(ct.user + ct.system)
                rt._last_sample_wall = time.perf_counter()
                rt._last_metrics_at = 0.0
            except Exception:
                rt.ps_proc = None

            rt.reader_task = asyncio.create_task(self._stdout_reader(instance_id, proc))
            rt.waiter_task = asyncio.create_task(self._process_waiter(instance_id, proc))

        await self._notify_state()
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
                try:
                    await self._append_log(instance_id, line)
                except Exception as line_exc:
                    logger.debug("Log handler error for %s: %s", instance_id, line_exc)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.debug("Stdout reader error for %s: %s", instance_id, exc)

    async def _process_waiter(self, instance_id: str, proc: asyncio.subprocess.Process) -> None:
        try:
            code = await proc.wait()
            rt = self.get_runtime(instance_id)
            was_stopping = rt.status == "stopping"
            if rt.process is proc:
                rt.status = "offline"
                rt.started_at = None
                rt.reset_runtime_metrics()
                rt.ps_proc = None
            await self._append_log(
                instance_id,
                f"\x1b[90m[SUPERVISOR] Process exited with return code {code}.\x1b[0m",
            )
            # 0xC0000135 (3221225781 / -1073741515) = STATUS_DLL_NOT_FOUND (missing Visual C++ runtime DLLs)
            if not was_stopping and code in (3221225781, -1073741515):
                await self._append_log(
                    instance_id,
                    "\x1b[33m[AUTO-HEAL]\x1b[0m Detected missing Visual C++ Runtime DLL (0xC0000135). "
                    "Auto-installing VC++ Runtime & restarting instance...",
                )
                await asyncio.to_thread(ensure_vc_redist, True)
                await asyncio.to_thread(sync_vc_dlls_to_directory, self.instance_dir(instance_id))
                asyncio.create_task(self.start_instance(instance_id))
            await self._notify_state()
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
                rt.reset_runtime_metrics()
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
            rt.reset_runtime_metrics()
            rt.ps_proc = None

        await self._notify_state()
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
            rt.reset_runtime_metrics()
            rt.ps_proc = None

        await self._notify_state()
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

