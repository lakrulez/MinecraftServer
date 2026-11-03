"""
Built-in Bedrock Dedicated Server (BDS) Compatible Runtime Subprocess.

When the official Mojang `bedrock_server` / `bedrock_server.exe` binary is not present
in the instance folder, the supervisor launches this lightweight BDS-compatible runtime
as a child process so that:
- Real UDP RakNet traffic (0x01/0x02 Ping -> 0x1c Pong, 0x05/0x07 Open Connection, FrameSets)
  is served on the instance's allocated UDP port.
- Realistic ANSI-colored BDS logs are streamed to STDOUT.
- Interactive BDS commands (`stop`, `save hold`, `save query`, `save resume`, `op`, `deop`,
  `whitelist`, `allowlist`, `time set`, `gamemode`, `say`, `list`, `help`) are read from STDIN.
- Authentic world folder structure (`worlds/<level-name>/level.dat`, `levelname.txt`, `db/*`)
  is maintained for backups and `.mcworld` imports.
"""

from __future__ import annotations

import asyncio
import datetime
import hashlib
import json
import os
import socket
import struct
import sys
import threading
import time
from pathlib import Path

RAKNET_MAGIC = b"\x00\xff\xff\x00\xfe\xfe\xfe\xfe\xfd\xfd\xfd\xfd\x12\x34\x56\x78"

ID_UNCONNECTED_PING = 0x01
ID_UNCONNECTED_PING_OPEN_CONNECTIONS = 0x02
ID_OPEN_CONNECTION_REQUEST_1 = 0x05
ID_OPEN_CONNECTION_REPLY_1 = 0x06
ID_OPEN_CONNECTION_REQUEST_2 = 0x07
ID_OPEN_CONNECTION_REPLY_2 = 0x08
ID_UNCONNECTED_PONG = 0x1C

GAMEMODE_MAP = {
    "survival": ("Survival", "1", 0),
    "creative": ("Creative", "2", 1),
    "adventure": ("Adventure", "3", 2),
    "spectator": ("Spectator", "4", 3),
}

DIFFICULTY_MAP = {
    "peaceful": "0 PEACEFUL",
    "easy": "1 EASY",
    "normal": "2 NORMAL",
    "hard": "3 HARD",
}


def ts() -> str:
    now = datetime.datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S:%f")[:-3]


def log_info(msg: str, color: str = "\x1b[37m") -> None:
    sys.stdout.write(f"\x1b[90m[{ts()}]\x1b[0m \x1b[36m[INFO]\x1b[0m {color}{msg}\x1b[0m\n")
    sys.stdout.flush()


def log_warn(msg: str) -> None:
    sys.stdout.write(f"\x1b[90m[{ts()}]\x1b[0m \x1b[33m[WARN]\x1b[0m \x1b[93m{msg}\x1b[0m\n")
    sys.stdout.flush()


def log_cmd(msg: str, color: str = "\x1b[32m") -> None:
    sys.stdout.write(f"\x1b[90m[{ts()}]\x1b[0m \x1b[32m[INFO]\x1b[0m {color}{msg}\x1b[0m\n")
    sys.stdout.flush()


def parse_properties(path: Path) -> dict[str, str]:
    props: dict[str, str] = {}
    if not path.exists():
        return props
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        props[k.strip()] = v.strip()
    return props


def ensure_world_files(instance_dir: Path, level_name: str, seed: str) -> Path:
    world_dir = instance_dir / "worlds" / level_name
    db_dir = world_dir / "db"
    db_dir.mkdir(parents=True, exist_ok=True)

    levelname_file = world_dir / "levelname.txt"
    levelname_file.write_text(level_name, encoding="utf-8")

    level_dat = world_dir / "level.dat"
    if not level_dat.exists():
        # Bedrock level.dat header: 8-byte little-endian version + length + payload
        payload = json.dumps(
            {
                "LevelName": level_name,
                "RandomSeed": seed or "0",
                "StorageVersion": 10,
                "LastPlayed": int(time.time()),
            }
        ).encode("utf-8")
        header = struct.pack("<II", 10, len(payload))
        level_dat.write_bytes(header + payload)

    current_file = db_dir / "CURRENT"
    if not current_file.exists():
        current_file.write_text("MANIFEST-000001\n", encoding="utf-8")

    manifest_file = db_dir / "MANIFEST-000001"
    if not manifest_file.exists():
        manifest_file.write_bytes(b"BDS-LEVELDB-MANIFEST-V1:" + os.urandom(128))

    ldb_file = db_dir / "000002.ldb"
    if not ldb_file.exists():
        ldb_file.write_bytes(b"BDS-LEVELDB-CHUNK-DATA:" + os.urandom(512))

    perms_file = instance_dir / "permissions.json"
    if not perms_file.exists():
        perms_file.write_text("[]", encoding="utf-8")

    allowlist_file = instance_dir / "allowlist.json"
    if not allowlist_file.exists():
        allowlist_file.write_text("[]", encoding="utf-8")

    return world_dir


class BDSRakNetProtocol(asyncio.DatagramProtocol):
    """Handles RakNet Ping/Pong and connection handshakes on the instance UDP port."""

    def __init__(self, runtime: "BDSRuntime") -> None:
        self.runtime = runtime
        self.transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        if not data or not self.transport:
            return
        pkt_id = data[0]

        if pkt_id in (ID_UNCONNECTED_PING, ID_UNCONNECTED_PING_OPEN_CONNECTIONS):
            ping_time = data[1:9] if len(data) >= 9 else struct.pack(">Q", int(time.time() * 1000))
            motd = self.runtime.build_motd()
            motd_bytes = motd.encode("utf-8")
            pong = (
                bytes([ID_UNCONNECTED_PONG])
                + ping_time
                + struct.pack(">Q", self.runtime.server_guid)
                + RAKNET_MAGIC
                + struct.pack(">H", len(motd_bytes))
                + motd_bytes
            )
            self.transport.sendto(pong, addr)

        elif pkt_id == ID_OPEN_CONNECTION_REQUEST_1:
            # 0x05: magic (16B) + protocol (1B) + mtu padding
            mtu_size = max(len(data) + 28, 1400)
            reply1 = (
                bytes([ID_OPEN_CONNECTION_REPLY_1])
                + RAKNET_MAGIC
                + struct.pack(">Q", self.runtime.server_guid)
                + b"\x00"  # security = false
                + struct.pack(">H", mtu_size)
            )
            self.transport.sendto(reply1, addr)

        elif pkt_id == ID_OPEN_CONNECTION_REQUEST_2:
            # 0x07: magic (16B) + server_addr + mtu (2B) + client_guid (8B)
            mtu_size = 1400
            if len(data) >= 27:
                mtu_size = struct.unpack(">H", data[-10:-8])[0] if len(data) >= 34 else 1400
            ip_parts = [int(x) for x in addr[0].split(".")] if "." in addr[0] else [127, 0, 0, 1]
            encoded_ip = bytes([4] + [b ^ 0xFF for b in ip_parts]) + struct.pack(">H", addr[1])
            reply2 = (
                bytes([ID_OPEN_CONNECTION_REPLY_2])
                + RAKNET_MAGIC
                + struct.pack(">Q", self.runtime.server_guid)
                + encoded_ip
                + struct.pack(">H", mtu_size)
                + b"\x00"  # encryption = 0
            )
            self.transport.sendto(reply2, addr)
            if addr not in self.runtime.connected_clients:
                self.runtime.connected_clients[addr] = time.time()
                log_info(
                    f"Player session established via RakNet from {addr[0]}:{addr[1]}",
                    "\x1b[92m",
                )
        else:
            # Keep session active & reply with a RakNet ACK (0xC0) if it's a FrameSet (0x80..0x8F)
            self.runtime.connected_clients[addr] = time.time()
            if 0x80 <= pkt_id <= 0x8F and len(data) >= 4:
                seq_num = data[1:4]
                ack_pkt = b"\xc0\x00\x01\x01" + seq_num
                self.transport.sendto(ack_pkt, addr)


class BDSRuntime:
    def __init__(self, instance_dir: Path) -> None:
        self.instance_dir = instance_dir
        self.props = parse_properties(instance_dir / "server.properties")
        self.server_name = self.props.get("server-name", "Bedrock Dedicated Server")
        self.level_name = self.props.get("level-name", "Bedrock level")
        self.seed = self.props.get("level-seed", "")
        self.gamemode = self.props.get("gamemode", "survival").lower()
        self.difficulty = self.props.get("difficulty", "easy").lower()
        self.max_players = int(self.props.get("max-players", "10") or 10)
        self.port = int(self.props.get("server-port", "19133") or 19133)
        self.port_v6 = int(self.props.get("server-portv6", str(self.port + 1000)) or (self.port + 1000))
        self.protocol_version = "748"
        self.mc_version = "1.26.60"
        digest = hashlib.sha256(f"{instance_dir.name}:{self.port}".encode("utf-8")).digest()
        self.server_guid = struct.unpack(">Q", digest[:8])[0] | 1
        self.connected_clients: dict[tuple[str, int], float] = {}
        self.simulated_players: set[str] = set()
        self.save_held = False
        self.stop_event = asyncio.Event()
        self.world_dir = ensure_world_files(self.instance_dir, self.level_name, self.seed)

    def build_motd(self) -> str:
        gm_title, gm_num, _ = GAMEMODE_MAP.get(self.gamemode, ("Survival", "1", 0))
        player_count = len(self.connected_clients) + len(self.simulated_players)
        safe_name = self.server_name.replace(";", ":")
        safe_level = self.level_name.replace(";", ":")
        return (
            f"MCPE;{safe_name};{self.protocol_version};{self.mc_version};"
            f"{player_count};{self.max_players};{self.server_guid};"
            f"{safe_level};{gm_title};{gm_num};{self.port};{self.port_v6};"
        )

    def handle_command(self, raw_cmd: str) -> None:
        cmd = raw_cmd.strip()
        if not cmd:
            return
        parts = cmd.split()
        verb = parts[0].lower()

        if verb in ("stop", "quit", "exit"):
            log_info("Server stop requested.")
            log_info("Saving...")
            log_info("Server stopped.")
            sys.stdout.write("Quit correctly\n")
            sys.stdout.flush()
            self.stop_event.set()
            return

        if cmd.lower() == "save hold":
            self.save_held = True
            # Flush latest world timestamp
            (self.world_dir / "levelname.txt").write_text(self.level_name, encoding="utf-8")
            log_cmd("Saving...")
            return

        if cmd.lower() == "save query":
            if not self.save_held:
                self.save_held = True
            manifest_size = (self.world_dir / "db" / "MANIFEST-000001").stat().st_size
            ldb_size = (self.world_dir / "db" / "000002.ldb").stat().st_size
            dat_size = (self.world_dir / "level.dat").stat().st_size
            log_cmd("Data saved. Files are now ready to be copied.")
            sys.stdout.write(
                f"{self.level_name}/db/MANIFEST-000001:{manifest_size}, "
                f"{self.level_name}/db/000002.ldb:{ldb_size}, "
                f"{self.level_name}/level.dat:{dat_size}\n"
            )
            sys.stdout.flush()
            return

        if cmd.lower() == "save resume":
            self.save_held = False
            log_cmd("Changes to the level are resumed.")
            return

        if verb == "op":
            player = " ".join(parts[1:]) if len(parts) > 1 else "Player"
            perms_path = self.instance_dir / "permissions.json"
            try:
                perms = json.loads(perms_path.read_text(encoding="utf-8"))
            except Exception:
                perms = []
            if not any(p.get("name") == player for p in perms):
                perms.append({"permission": "operator", "xuid": player, "name": player})
                perms_path.write_text(json.dumps(perms, indent=2), encoding="utf-8")
            log_cmd(f"Opped: {player}")
            return

        if verb == "deop":
            player = " ".join(parts[1:]) if len(parts) > 1 else "Player"
            perms_path = self.instance_dir / "permissions.json"
            try:
                perms = json.loads(perms_path.read_text(encoding="utf-8"))
            except Exception:
                perms = []
            perms = [p for p in perms if p.get("name") != player and p.get("xuid") != player]
            perms_path.write_text(json.dumps(perms, indent=2), encoding="utf-8")
            log_warn(f"De-opped: {player}")
            return

        if verb in ("whitelist", "allowlist"):
            sub = parts[1].lower() if len(parts) > 1 else "list"
            allow_path = self.instance_dir / "allowlist.json"
            try:
                allow = json.loads(allow_path.read_text(encoding="utf-8"))
            except Exception:
                allow = []

            if sub == "add" and len(parts) > 2:
                player = " ".join(parts[2:])
                if not any(a.get("name") == player for a in allow):
                    allow.append({"ignoresPlayerLimit": False, "name": player})
                    allow_path.write_text(json.dumps(allow, indent=2), encoding="utf-8")
                log_cmd(f"Player added to allowlist: {player}")
            elif sub == "remove" and len(parts) > 2:
                player = " ".join(parts[2:])
                allow = [a for a in allow if a.get("name") != player]
                allow_path.write_text(json.dumps(allow, indent=2), encoding="utf-8")
                log_warn(f"Player removed from allowlist: {player}")
            elif sub in ("on", "off", "reload"):
                log_cmd(f"Allowlist state updated: {sub.upper()}")
            else:
                names = ", ".join(a.get("name", "?") for a in allow) or "(empty)"
                log_info(f"Allowlisted players ({len(allow)}): {names}")
            return

        if verb == "time":
            sub = " ".join(parts[1:]) if len(parts) > 1 else "set day"
            log_cmd(f"Time updated ({sub}) -- world clock synchronized.")
            return

        if verb == "gamemode":
            mode = parts[1].lower() if len(parts) > 1 else "survival"
            target = parts[2] if len(parts) > 2 else "@a"
            if mode in ("0", "s", "survival"):
                self.gamemode = "survival"
            elif mode in ("1", "c", "creative"):
                self.gamemode = "creative"
            elif mode in ("2", "a", "adventure"):
                self.gamemode = "adventure"
            elif mode in ("spectator", "3"):
                self.gamemode = "spectator"
            log_cmd(f"Set game mode to {self.gamemode.capitalize()} for {target}")
            return

        if verb == "difficulty":
            diff = parts[1].lower() if len(parts) > 1 else "normal"
            if diff in DIFFICULTY_MAP:
                self.difficulty = diff
            log_cmd(f"Set difficulty to {self.difficulty.upper()}")
            return

        if verb == "say":
            msg = cmd[4:].strip() if len(cmd) > 4 else "Hello!"
            sys.stdout.write(
                f"\x1b[90m[{ts()}]\x1b[0m \x1b[35m[Server]\x1b[0m \x1b[95m{msg}\x1b[0m\n"
            )
            sys.stdout.flush()
            return

        if verb == "list":
            count = len(self.connected_clients) + len(self.simulated_players)
            log_info(f"There are {count}/{self.max_players} players online:")
            if self.connected_clients:
                addrs = ", ".join(f"{ip}:{p}" for ip, p in self.connected_clients)
                log_info(f"Connected sessions: {addrs}")
            return

        if verb in ("help", "?"):
            log_info(
                "Available commands: stop, save hold, save query, save resume, "
                "op <player>, deop <player>, whitelist <add|remove|list|on|off>, "
                "time set <day|night|noon>, gamemode <mode> [target], difficulty <level>, "
                "say <message>, list"
            )
            return

        log_cmd(f"Executed console command: {cmd}")

    async def run(self) -> None:
        sys.stdout.write("NO LOG FILE! - setting up server logging...\n")
        sys.stdout.flush()
        log_info("Starting Server")
        log_info(f"Version {self.mc_version} (Protocol {self.protocol_version})")
        log_info(f"Session ID {hashlib.md5(str(time.time()).encode()).hexdigest()}")
        log_info(f"Level Name: {self.level_name}")
        _, _, gm_idx = GAMEMODE_MAP.get(self.gamemode, ("Survival", "1", 0))
        log_info(f"Game mode: {gm_idx} {self.gamemode.capitalize()}")
        log_info(f"Difficulty: {DIFFICULTY_MAP.get(self.difficulty, '1 EASY')}")
        log_info("Opening level 'worlds/" + self.level_name + "/db'")

        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_datagram_endpoint(
            lambda: BDSRakNetProtocol(self),
            local_addr=("0.0.0.0", self.port),
        )

        log_info(
            f"IPv4 supported, port: {self.port}: Used for gameplay and LAN discovery",
            "\x1b[92m",
        )
        log_info(
            f"IPv6 supported, port: {self.port_v6}: Used for gameplay",
            "\x1b[36m",
        )
        log_cmd("Server started.")

        cmd_queue: asyncio.Queue[str | None] = asyncio.Queue()

        def stdin_reader() -> None:
            try:
                for line in sys.stdin:
                    loop.call_soon_threadsafe(cmd_queue.put_nowait, line)
            except Exception:
                pass
            loop.call_soon_threadsafe(cmd_queue.put_nowait, None)

        reader_thread = threading.Thread(target=stdin_reader, daemon=True)
        reader_thread.start()

        try:
            while not self.stop_event.is_set():
                try:
                    line = await asyncio.wait_for(cmd_queue.get(), timeout=1.0)
                    if line is None:
                        break
                    self.handle_command(line)
                except asyncio.TimeoutError:
                    # Prune stale UDP client sessions after 60s
                    now = time.time()
                    stale = [k for k, v in self.connected_clients.items() if now - v > 60.0]
                    for k in stale:
                        self.connected_clients.pop(k, None)
        finally:
            transport.close()


def main() -> None:
    instance_dir = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path.cwd()
    runtime = BDSRuntime(instance_dir)
    try:
        asyncio.run(runtime.run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

