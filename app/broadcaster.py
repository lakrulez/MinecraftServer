"""
Async UDP RakNet LAN Discovery & Switch/Console Bridge Proxy (`/app/broadcaster.py`).

Responsibilities:
- Binds to UDP port 19132 (`0.0.0.0:19132`) to listen for Minecraft Bedrock LAN broadcast
  pings (`ID_UNCONNECTED_PING` = 0x01, `ID_UNCONNECTED_PING_OPEN_CONNECTIONS` = 0x02).
- Responds with a valid RakNet `ID_UNCONNECTED_PONG` (0x1c) containing the Bedrock MOTD format:
  `MCPE;ServerName;Protocol;Version;PlayerCount;MaxPlayers;ServerGUID;SubMOTD;GameMode;GameModeNumeric;IPv4Port;IPv6Port;`
- Transparently routes and forwards local player connection packets between consoles
  (incoming on UDP 19132) and the selected target BDS instance port (e.g., 19133, 19134)
  using per-client ephemeral UDP session sockets.
- Supports zero-downtime hot-switching of the active target instance and live enable/disable
  toggling from the Web GUI.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import socket
import struct
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from cryptography.hazmat.primitives import padding as sym_padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

logger = logging.getLogger("bedrock.broadcaster")

RAKNET_MAGIC = b"\x00\xff\xff\x00\xfe\xfe\xfe\xfe\xfd\xfd\xfd\xfd\x12\x34\x56\x78"
NETHERNET_KEY = hashlib.sha256(struct.pack("<Q", 0xDEADBEEF)).digest()
_NETHERNET_CIPHER = Cipher(algorithms.AES(NETHERNET_KEY), modes.ECB())


def parse_nethernet_header(data: bytes) -> dict[str, int] | None:
    """
    Decrypt the first 1-2 AES-256-ECB blocks of a NetherNet UDP 7551 packet (after the 32-byte HMAC)
    to inspect its packet_id (0=DiscoveryRequest, 1=DiscoveryResponse, 2=DiscoveryMessage),
    sender_network_id, and (for packet_id == 2) recipient_network_id.
    """
    if len(data) < 48 or (len(data) - 32) % 16 != 0:
        return None
    try:
        decryptor = _NETHERNET_CIPHER.decryptor()
        head_enc = data[32 : min(len(data), 64)]
        plain = decryptor.update(head_enc)
        if len(plain) < 16:
            return None
        length, pkt_id, sender_id = struct.unpack("<HHQ", plain[:12])
        info: dict[str, int] = {
            "length": length,
            "pkt_id": pkt_id,
            "sender_id": sender_id,
        }
        if pkt_id == 2 and len(plain) >= 28:
            info["recipient_id"] = struct.unpack("<Q", plain[20:28])[0]
        return info
    except Exception:
        return None


def build_nethernet_discovery_probe(sender_id: int = 0xBEEF7551CAFE1913) -> bytes:
    """Build an encrypted NetherNet `DiscoveryRequestPacket` (packet_id=0) to probe local BDS instances."""
    plaintext = struct.pack("<HHQQ", 20, 0, sender_id, 0)
    chk = hmac.new(NETHERNET_KEY, plaintext, hashlib.sha256).digest()
    padder = sym_padding.PKCS7(128).padder()
    padded = padder.update(plaintext) + padder.finalize()
    encryptor = _NETHERNET_CIPHER.encryptor()
    return chk + encryptor.update(padded) + encryptor.finalize()

ID_UNCONNECTED_PING = 0x01
ID_UNCONNECTED_PING_OPEN_CONNECTIONS = 0x02
ID_OPEN_CONNECTION_REQUEST_1 = 0x05
ID_OPEN_CONNECTION_REPLY_1 = 0x06
ID_OPEN_CONNECTION_REQUEST_2 = 0x07
ID_OPEN_CONNECTION_REPLY_2 = 0x08
ID_UNCONNECTED_PONG = 0x1C

GAMEMODE_TABLE: dict[str, tuple[str, str]] = {
    "survival": ("Survival", "1"),
    "creative": ("Creative", "2"),
    "adventure": ("Adventure", "3"),
    "spectator": ("Spectator", "4"),
}


def get_local_lan_ip() -> str:
    """Detect the primary LAN IPv4 address of this host."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception:
        try:
            return socket.gethostbyname(socket.gethostname())
        except Exception:
            return "127.0.0.1"


def compute_instance_guid(instance_id: str, port: int) -> int:
    """Generate a deterministic 64-bit unsigned ServerGUID for an instance."""
    digest = hashlib.sha256(f"{instance_id}:{port}".encode("utf-8")).digest()
    return struct.unpack(">Q", digest[:8])[0] | 1


def build_bedrock_motd(
    server_name: str,
    protocol: str | int = "748",
    version: str = "1.26.60",
    player_count: int = 0,
    max_players: int = 10,
    server_guid: int = 13253860892328930865,
    sub_motd: str = "Bedrock LAN Proxy",
    gamemode: str = "survival",
    ipv4_port: int = 19132,
    ipv6_port: int = 19133,
) -> str:
    """
    Construct the standard 12-field Bedrock MOTD payload string:
    `MCPE;ServerName;Protocol;Version;PlayerCount;MaxPlayers;ServerGUID;SubMOTD;GameMode;GameModeNumeric;IPv4Port;IPv6Port;`
    """
    gm_key = str(gamemode).lower().strip()
    gm_label, gm_num = GAMEMODE_TABLE.get(gm_key, ("Survival", "1"))
    safe_name = str(server_name).replace(";", ":") or "Bedrock Server"
    safe_sub = str(sub_motd).replace(";", ":") or "Bedrock World"
    return (
        f"MCPE;{safe_name};{protocol};{version};"
        f"{player_count};{max_players};{server_guid};"
        f"{safe_sub};{gm_label};{gm_num};{ipv4_port};{ipv6_port};"
    )


def pack_unconnected_pong(ping_time_bytes: bytes, server_guid: int, motd: str) -> bytes:
    """
    Pack a RakNet `ID_UNCONNECTED_PONG` (0x1c) packet:
    - 1 byte: 0x1c
    - 8 bytes: ping timestamp echoed from client
    - 8 bytes: big-endian uint64 server_guid
    - 16 bytes: RakNet OFFLINE_MESSAGE_DATA_ID magic
    - 2 bytes: big-endian uint16 MOTD UTF-8 byte length
    - N bytes: MOTD UTF-8 string
    """
    if len(ping_time_bytes) != 8:
        ping_time_bytes = struct.pack(">Q", int(time.time() * 1000))
    motd_bytes = motd.encode("utf-8")
    return (
        bytes([ID_UNCONNECTED_PONG])
        + ping_time_bytes
        + struct.pack(">Q", server_guid & 0xFFFFFFFFFFFFFFFF)
        + RAKNET_MAGIC
        + struct.pack(">H", len(motd_bytes))
        + motd_bytes
    )


def parse_unconnected_pong(data: bytes) -> dict[str, Any] | None:
    """Parse a RakNet 0x1c `ID_UNCONNECTED_PONG` packet into its header fields and MOTD parts."""
    if len(data) < 35 or data[0] != ID_UNCONNECTED_PONG:
        return None
    ping_time = struct.unpack(">Q", data[1:9])[0]
    server_guid = struct.unpack(">Q", data[9:17])[0]
    magic = data[17:33]
    if magic != RAKNET_MAGIC:
        return None
    motd_len = struct.unpack(">H", data[33:35])[0]
    if len(data) < 35 + motd_len:
        return None
    motd_str = data[35 : 35 + motd_len].decode("utf-8", errors="replace")
    parts = motd_str.split(";")
    result: dict[str, Any] = {
        "ping_time": ping_time,
        "server_guid": server_guid,
        "motd": motd_str,
        "parts": parts,
    }
    if len(parts) >= 12 and parts[0] == "MCPE":
        result.update(
            {
                "edition": parts[0],
                "server_name": parts[1],
                "protocol": parts[2],
                "version": parts[3],
                "player_count": int(parts[4]) if parts[4].isdigit() else 0,
                "max_players": int(parts[5]) if parts[5].isdigit() else 10,
                "motd_guid": int(parts[6]) if parts[6].isdigit() else server_guid,
                "sub_motd": parts[7],
                "gamemode": parts[8],
                "gamemode_numeric": parts[9],
                "ipv4_port": int(parts[10]) if parts[10].isdigit() else 19132,
                "ipv6_port": int(parts[11]) if parts[11].isdigit() else 19133,
            }
        )
    return result


@dataclass
class TargetInstanceConfig:
    """Metadata of the BDS instance currently mapped to the LAN broadcaster."""

    id: str
    display_name: str
    world_name: str
    port: int
    ipv6_port: int = 20133
    nethernet_udp_port: int | None = None
    gamemode: str = "survival"
    max_players: int = 10
    protocol: str = "748"
    version: str = "1.26.60"
    player_count: int = 0
    server_guid: int = 0

    def __post_init__(self) -> None:
        if not self.server_guid:
            self.server_guid = compute_instance_guid(self.id, self.port)


class _BackendSessionProtocol(asyncio.DatagramProtocol):
    """Per-client ephemeral UDP socket protocol communicating with `127.0.0.1:<target_port>`."""

    def __init__(
        self,
        session: "ClientBridgeSession",
        broadcaster: "RakNetLANBroadcaster",
    ) -> None:
        self.session = session
        self.broadcaster = broadcaster

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.session.transport = transport  # type: ignore[assignment]
        # Flush any packets queued while the ephemeral socket was opening
        while self.session.pending_packets:
            pkt = self.session.pending_packets.pop(0)
            self.session.transport.sendto(pkt)
            self.broadcaster.packets_to_server += 1

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        self.session.last_active = time.time()
        self.broadcaster.packets_to_client += 1
        self.broadcaster.bytes_forwarded += len(data)

        # If backend sends an unconnected pong on a session socket, rewrite port to proxy bind_port
        if data and data[0] == ID_UNCONNECTED_PONG:
            parsed = parse_unconnected_pong(data)
            if parsed and len(parsed.get("parts", [])) >= 12:
                parts = list(parsed["parts"])
                parts[10] = str(self.broadcaster.bind_port)
                rewritten_motd = ";".join(parts)
                data = pack_unconnected_pong(data[1:9], parsed["server_guid"], rewritten_motd)

        if self.broadcaster.transport:
            self.broadcaster.transport.sendto(data, self.session.client_addr)

    def error_received(self, exc: Exception) -> None:
        logger.debug("Backend UDP session error for %s: %s", self.session.client_addr, exc)


@dataclass
class ClientBridgeSession:
    """Represents a single console/player UDP session routed through the proxy."""

    client_addr: tuple[str, int]
    target_port: int
    created_at: float = field(default_factory=time.time)
    last_active: float = field(default_factory=time.time)
    transport: asyncio.DatagramTransport | None = None
    pending_packets: list[bytes] = field(default_factory=list)
    packets_sent: int = 0

    def send_to_backend(self, data: bytes, broadcaster: "RakNetLANBroadcaster") -> None:
        self.last_active = time.time()
        self.packets_sent += 1
        broadcaster.bytes_forwarded += len(data)
        if self.transport is not None:
            self.transport.sendto(data)
            broadcaster.packets_to_server += 1
        else:
            if len(self.pending_packets) < 64:
                self.pending_packets.append(data)

    def close(self) -> None:
        if self.transport is not None:
            try:
                self.transport.close()
            except Exception:
                pass
            self.transport = None
        self.pending_packets.clear()


class _MainProxyProtocol(asyncio.DatagramProtocol):
    """Main UDP 19132 listener handling RakNet LAN discovery pings and client traffic."""

    def __init__(self, broadcaster: "RakNetLANBroadcaster") -> None:
        self.broadcaster = broadcaster

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.broadcaster.transport = transport  # type: ignore[assignment]

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        self.broadcaster.handle_incoming_datagram(data, addr)

    def error_received(self, exc: Exception) -> None:
        # On Windows UDP sockets, ICMP Port Unreachable (WinError 10054) can trigger error_received
        logger.debug("Main UDP listener socket notice: %s", exc)


class _NetherNetBackendSessionProtocol(asyncio.DatagramProtocol):
    """Per-client ephemeral UDP socket forwarding NetherNet packets to `127.0.0.1:<nethernet_udp_port>`."""

    def __init__(
        self,
        session: ClientBridgeSession,
        broadcaster: "RakNetLANBroadcaster",
    ) -> None:
        self.session = session
        self.broadcaster = broadcaster

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.session.transport = transport  # type: ignore[assignment]
        while self.session.pending_packets:
            pkt = self.session.pending_packets.pop(0)
            self.session.transport.sendto(pkt)
            self.broadcaster.packets_to_server += 1

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        self.session.last_active = time.time()
        hdr = parse_nethernet_header(data)
        if hdr is not None:
            if hdr["pkt_id"] == 1 and hdr["sender_id"]:
                self.broadcaster.nethernet_id_to_port[hdr["sender_id"]] = self.session.target_port
            elif hdr["pkt_id"] == 2 and hdr["sender_id"] == 0:
                # Ignore empty NACK reset packet from non-recipient instance
                return
        self.broadcaster.packets_to_client += 1
        self.broadcaster.pongs_sent += 1
        self.broadcaster.bytes_forwarded += len(data)
        if self.broadcaster.nethernet_transport:
            self.broadcaster.nethernet_transport.sendto(data, self.session.client_addr)

    def error_received(self, exc: Exception) -> None:
        logger.debug("NetherNet backend session error for %s: %s", self.session.client_addr, exc)


class _NetherNetProxyProtocol(asyncio.DatagramProtocol):
    """Dual-stack UDP 7551 listener handling Bedrock 1.26+ NetherNet LAN Discovery & WebRTC signaling."""

    def __init__(self, broadcaster: "RakNetLANBroadcaster") -> None:
        self.broadcaster = broadcaster

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.broadcaster.nethernet_transport = transport  # type: ignore[assignment]

    def datagram_received(self, data: bytes, addr: Any) -> None:
        self.broadcaster.handle_nethernet_datagram(data, addr)

    def error_received(self, exc: Exception) -> None:
        logger.debug("NetherNet UDP 7551 listener notice: %s", exc)


class _ProbeProtocol(asyncio.DatagramProtocol):
    """Internal probe socket that syncs live ServerGUID, NetherNet ID, and player count from BDS."""

    def __init__(self, broadcaster: "RakNetLANBroadcaster") -> None:
        self.broadcaster = broadcaster

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        hdr = parse_nethernet_header(data)
        if hdr is not None and hdr["pkt_id"] == 1 and hdr["sender_id"]:
            self.broadcaster.nethernet_id_to_port[hdr["sender_id"]] = addr[1]
            return

        parsed = parse_unconnected_pong(data)
        if not parsed or not self.broadcaster.target:
            return
        if addr[1] == self.broadcaster.target.port:
            self.broadcaster.target.server_guid = parsed["server_guid"]
            if "protocol" in parsed:
                self.broadcaster.target.protocol = str(parsed["protocol"])
            if "version" in parsed:
                self.broadcaster.target.version = str(parsed["version"])
            if "player_count" in parsed:
                self.broadcaster.target.player_count = int(parsed["player_count"])


class RakNetLANBroadcaster:
    """
    Asynchronous UDP RakNet (19132) & NetherNet (7551) LAN Discovery Responder & Packet Router.
    Binds to UDP 19132 and UDP 7551, broadcasts all online instances over NetherNet LAN Discovery,
    and routes signaling/gameplay traffic to the selected or addressed BDS instance.
    """

    def __init__(
        self,
        bind_host: str = "0.0.0.0",
        bind_port: int = 19132,
        nethernet_bind_port: int = 7551,
        session_timeout: float = 60.0,
        event_callback: Callable[[str, str], None] | None = None,
    ) -> None:
        self.bind_host = bind_host
        self.bind_port = bind_port
        self.nethernet_bind_port = nethernet_bind_port
        self.session_timeout = session_timeout
        self.event_callback = event_callback

        self.enabled: bool = True
        self.target: TargetInstanceConfig | None = None
        self.online_targets: dict[str, TargetInstanceConfig] = {}
        self.nethernet_id_to_port: dict[int, int] = {}
        self.transport: asyncio.DatagramTransport | None = None
        self.nethernet_transport: asyncio.DatagramTransport | None = None
        self.probe_transport: asyncio.DatagramTransport | None = None
        self._housekeeping_task: asyncio.Task[None] | None = None
        self.bound_ok: bool = False
        self.nethernet_bound_ok: bool = False
        self.bind_error: str | None = None

        self.sessions: dict[Any, ClientBridgeSession] = {}
        self.nethernet_sessions: dict[tuple[Any, int], ClientBridgeSession] = {}
        self.pings_received: int = 0
        self.pongs_sent: int = 0
        self.packets_to_server: int = 0
        self.packets_to_client: int = 0
        self.bytes_forwarded: int = 0
        self.last_ping_at: float | None = None
        self.last_ping_from: str | None = None
        self.lan_ip: str = get_local_lan_ip()

    async def start(self) -> None:
        """Bind UDP port 19132 (RakNet) and UDP port 7551 (NetherNet) and start housekeeping."""
        loop = asyncio.get_running_loop()
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            # Suppress Windows UDP WinError 10054 (WSAECONNRESET) on ICMP unreachable
            if hasattr(socket, "SIO_UDP_CONNRESET"):
                try:
                    sock.ioctl(socket.SIO_UDP_CONNRESET, False)  # type: ignore[attr-defined]
                except Exception:
                    pass
            sock.setblocking(False)
            sock.bind((self.bind_host, self.bind_port))

            transport, _ = await loop.create_datagram_endpoint(
                lambda: _MainProxyProtocol(self),
                sock=sock,
            )
            self.transport = transport  # type: ignore[assignment]
            self.bound_ok = True
            self.bind_error = None
            logger.info("RakNet LAN Broadcaster bound to UDP %s:%d", self.bind_host, self.bind_port)
        except Exception as exc:
            self.bound_ok = False
            self.bind_error = str(exc)
            logger.error("Failed to bind UDP %s:%d: %s", self.bind_host, self.bind_port, exc)

        # Bind UDP 7551 (dual-stack IPv6+IPv4 with SO_EXCLUSIVEADDRUSE so BDS instances
        # cleanly bind ephemeral NetherNet ports and let the proxy route UDP 7551)
        try:
            nn_sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
            nn_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
            if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                nn_sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            nn_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            if hasattr(socket, "SIO_UDP_CONNRESET"):
                try:
                    nn_sock.ioctl(socket.SIO_UDP_CONNRESET, False)  # type: ignore[attr-defined]
                except Exception:
                    pass
            nn_sock.setblocking(False)
            nn_sock.bind(("::", self.nethernet_bind_port))

            nn_transport, _ = await loop.create_datagram_endpoint(
                lambda: _NetherNetProxyProtocol(self),
                sock=nn_sock,
            )
            self.nethernet_transport = nn_transport  # type: ignore[assignment]
            self.nethernet_bound_ok = True
            logger.info("NetherNet LAN Proxy bound to dual-stack UDP [::]:%d", self.nethernet_bind_port)
        except Exception as exc:
            self.nethernet_bound_ok = False
            logger.warning("Failed to bind NetherNet UDP [::]:%d: %s", self.nethernet_bind_port, exc)

        try:
            probe_transport, _ = await loop.create_datagram_endpoint(
                lambda: _ProbeProtocol(self),
                local_addr=("127.0.0.1", 0),
            )
            self.probe_transport = probe_transport  # type: ignore[assignment]
        except Exception as exc:
            logger.debug("Failed to create internal probe socket: %s", exc)

        self._housekeeping_task = asyncio.create_task(self._housekeeping_loop())

    async def stop(self) -> None:
        """Close all proxy sockets and stop background tasks."""
        if self._housekeeping_task:
            self._housekeeping_task.cancel()
            try:
                await self._housekeeping_task
            except asyncio.CancelledError:
                pass
            self._housekeeping_task = None

        self._close_all_sessions()

        if self.probe_transport:
            self.probe_transport.close()
            self.probe_transport = None

        if self.nethernet_transport:
            self.nethernet_transport.close()
            self.nethernet_transport = None
        self.nethernet_bound_ok = False

        if self.transport:
            self.transport.close()
            self.transport = None
        self.bound_ok = False

    def _close_all_sessions(self) -> None:
        for session in list(self.sessions.values()):
            session.close()
        self.sessions.clear()
        self._close_nethernet_sessions()

    def _close_nethernet_sessions(self) -> None:
        for session in list(self.nethernet_sessions.values()):
            session.close()
        self.nethernet_sessions.clear()

    def set_enabled(self, enabled: bool) -> None:
        """Enable or disable LAN broadcasting & proxy routing."""
        if self.enabled == enabled:
            return
        self.enabled = enabled
        if not enabled:
            self._close_all_sessions()
        if self.event_callback and self.target:
            state_str = "ENABLED" if enabled else "DISABLED"
            self.event_callback(
                self.target.id,
                f"\x1b[36m[LAN-PROXY]\x1b[0m Broadcast beacon {state_str} on UDP :{self.bind_port} & :{self.nethernet_bind_port}",
            )

    def sync_online_instances(self, instances: list[dict[str, Any]]) -> None:
        """Keep the registry of all currently running BDS instances up-to-date for multi-server LAN discovery."""
        new_online: dict[str, TargetInstanceConfig] = {}
        active_nn_ports: set[int] = set()

        for inst in instances:
            if inst.get("status") != "online":
                continue
            iid = str(inst["id"])
            port = int(inst["port"])
            raw_nn = inst.get("nethernet_udp_port")
            nn_port = int(raw_nn) if raw_nn else None
            if nn_port:
                active_nn_ports.add(nn_port)
            new_online[iid] = TargetInstanceConfig(
                id=iid,
                display_name=str(inst.get("display_name", "Bedrock Server")),
                world_name=str(inst.get("world_name", "Bedrock level")),
                port=port,
                ipv6_port=int(inst.get("ipv6_port", port + 1000)),
                nethernet_udp_port=nn_port,
                gamemode=str(inst.get("gamemode", "survival")),
                max_players=int(inst.get("max_players", 10)),
                player_count=int(inst.get("players_online", 0)),
            )

        self.online_targets = new_online

        # Prune any NetherNet sessions or ID mappings pointing to ports that are no longer online
        for key in [k for k, sess in self.nethernet_sessions.items() if sess.target_port not in active_nn_ports]:
            sess = self.nethernet_sessions.pop(key, None)
            if sess:
                sess.close()
        for net_id in [nid for nid, p in self.nethernet_id_to_port.items() if p not in active_nn_ports]:
            self.nethernet_id_to_port.pop(net_id, None)

    def set_target_instance(self, instance_data: dict[str, Any] | None) -> None:
        """Hot-switch which BDS instance is mapped to UDP 19132 & primary NetherNet target."""
        if instance_data is None:
            self.target = None
            for session in list(self.sessions.values()):
                session.close()
            self.sessions.clear()
            return

        new_id = str(instance_data["id"])
        new_port = int(instance_data["port"])
        raw_nn_port = instance_data.get("nethernet_udp_port")
        new_nn_port = int(raw_nn_port) if raw_nn_port else None

        port_changed = self.target is None or self.target.port != new_port or self.target.id != new_id

        # Preserve already-synced server_guid if updating the same instance
        existing_guid = (
            self.target.server_guid
            if (self.target and self.target.id == new_id and not port_changed)
            else compute_instance_guid(new_id, new_port)
        )

        self.target = TargetInstanceConfig(
            id=new_id,
            display_name=str(instance_data.get("display_name", "Bedrock Server")),
            world_name=str(instance_data.get("world_name", "Bedrock level")),
            port=new_port,
            ipv6_port=int(instance_data.get("ipv6_port", new_port + 1000)),
            nethernet_udp_port=new_nn_port,
            gamemode=str(instance_data.get("gamemode", "survival")),
            max_players=int(instance_data.get("max_players", 10)),
            player_count=int(instance_data.get("players_online", 0)),
            server_guid=existing_guid,
        )

        if port_changed:
            for session in list(self.sessions.values()):
                session.close()
            self.sessions.clear()
            self._send_probe_ping()
            if self.event_callback:
                nn_info = f", NetherNet :7551 -> :{new_nn_port}" if new_nn_port else ""
                self.event_callback(
                    new_id,
                    f"\x1b[95m[LAN-PROXY]\x1b[0m Active LAN Broadcast mapped to "
                    f"'{self.target.display_name}' (UDP :{self.bind_port} -> 127.0.0.1:{new_port}{nn_info})",
                )

    def build_current_motd(self) -> str:
        """Build the Bedrock MOTD string for the currently selected target instance."""
        if not self.target:
            return build_bedrock_motd(
                server_name="Bedrock LAN Hub",
                sub_motd="No Instance Selected",
                ipv4_port=self.bind_port,
            )
        return build_bedrock_motd(
            server_name=self.target.display_name,
            protocol=self.target.protocol,
            version=self.target.version,
            player_count=self.target.player_count,
            max_players=self.target.max_players,
            server_guid=self.target.server_guid,
            sub_motd=f"{self.target.world_name} [LAN :{self.target.port}]",
            gamemode=self.target.gamemode,
            ipv4_port=self.bind_port,
            ipv6_port=self.target.ipv6_port,
        )

    def _get_online_nethernet_ports(self) -> list[int]:
        """Return all active NetherNet UDP ports across running BDS instances."""
        ports: list[int] = []
        if self.target and self.target.nethernet_udp_port:
            ports.append(self.target.nethernet_udp_port)
        for cfg in self.online_targets.values():
            if cfg.nethernet_udp_port and cfg.nethernet_udp_port not in ports:
                ports.append(cfg.nethernet_udp_port)
        return ports

    def _send_nethernet_to_port(self, addr: Any, target_nn_port: int, data: bytes) -> None:
        """Forward a NetherNet packet from `addr` to `127.0.0.1:<target_nn_port>` via a persistent session."""
        key = (addr, target_nn_port)
        session = self.nethernet_sessions.get(key)
        if session is None or session.target_port != target_nn_port:
            if session is not None:
                session.close()
            session = ClientBridgeSession(client_addr=addr, target_port=target_nn_port)
            self.nethernet_sessions[key] = session
            asyncio.create_task(self._open_nethernet_backend_session(key, session))
        session.send_to_backend(data, self)

    def handle_nethernet_datagram(self, data: bytes, addr: Any) -> None:
        """
        Process incoming NetherNet LAN discovery & signaling packets on UDP 7551.
        - DiscoveryRequests (pkt_id=0) are fanned out to ALL online BDS instances so every running
          server appears simultaneously in Minecraft's LAN Worlds list.
        - DiscoveryMessages (pkt_id=2, WebRTC connection signaling) are routed directly to the
          specific BDS instance matching `recipient_id` (the server the player clicked on).
        """
        if not data or not self.enabled or not self.nethernet_transport:
            return

        online_nn_ports = self._get_online_nethernet_ports()
        if not online_nn_ports:
            return

        host_str = str(addr[0]).replace("::ffff:", "")
        port_num = int(addr[1])

        # Ignore any packets originating from our own backend NetherNet ports
        if host_str in ("127.0.0.1", "::1", self.lan_ip) and port_num in online_nn_ports:
            return

        hdr = parse_nethernet_header(data)
        if hdr is not None and hdr["pkt_id"] == 1:
            # Ignore DiscoveryResponse packets coming from other clients on LAN
            return

        self.pings_received += 1
        self.last_ping_at = time.time()
        self.last_ping_from = f"{host_str}:{port_num}"

        # If this is a targeted WebRTC Signaling Message (pkt_id == 2), route to the addressed server
        if hdr is not None and hdr["pkt_id"] == 2:
            recipient_id = hdr.get("recipient_id", 0)
            routed_port = self.nethernet_id_to_port.get(recipient_id)
            if routed_port and routed_port in online_nn_ports:
                self._send_nethernet_to_port(addr, routed_port, data)
                return
            # Fallback if only 1 server is online or primary target has a port
            fallback_port = (
                self.target.nethernet_udp_port
                if (self.target and self.target.nethernet_udp_port in online_nn_ports)
                else online_nn_ports[0]
            )
            self._send_nethernet_to_port(addr, fallback_port, data)
            return

        # Discovery Request (pkt_id == 0): Fan out to ALL online BDS instances simultaneously!
        for nn_port in online_nn_ports:
            self._send_nethernet_to_port(addr, nn_port, data)

    async def _open_nethernet_backend_session(
        self,
        key: tuple[Any, int],
        session: ClientBridgeSession,
    ) -> None:
        loop = asyncio.get_running_loop()
        try:
            await loop.create_datagram_endpoint(
                lambda: _NetherNetBackendSessionProtocol(session, self),
                remote_addr=("127.0.0.1", session.target_port),
            )
        except Exception as exc:
            logger.debug(
                "Failed to open NetherNet backend UDP session to :%d: %s",
                session.target_port,
                exc,
            )
            self.nethernet_sessions.pop(key, None)

    def handle_incoming_datagram(self, data: bytes, addr: tuple[str, int]) -> None:
        """Process incoming UDP datagram on port 19132."""
        if not data or not self.enabled or not self.transport:
            return

        pkt_id = data[0]

        # 1. RakNet Broadcast Discovery Ping (0x01 or 0x02)
        if pkt_id in (ID_UNCONNECTED_PING, ID_UNCONNECTED_PING_OPEN_CONNECTIONS):
            self.pings_received += 1
            self.last_ping_at = time.time()
            self.last_ping_from = f"{addr[0]}:{addr[1]}"

            if self.target is None:
                return

            ping_time = data[1:9] if len(data) >= 9 else struct.pack(">Q", int(time.time() * 1000))
            motd = self.build_current_motd()
            pong_pkt = pack_unconnected_pong(ping_time, self.target.server_guid, motd)
            self.transport.sendto(pong_pkt, addr)
            self.pongs_sent += 1
            return

        # 2. Console / Client Connection & Gameplay Traffic -> Forward to Target Instance Port
        if self.target is None:
            return

        target_port = self.target.port
        session = self.sessions.get(addr)
        if session is None or session.target_port != target_port:
            if session is not None:
                session.close()
            session = ClientBridgeSession(client_addr=addr, target_port=target_port)
            self.sessions[addr] = session
            asyncio.create_task(self._open_backend_session(session))
            if self.event_callback:
                self.event_callback(
                    self.target.id,
                    f"\x1b[36m[LAN-PROXY]\x1b[0m Routing console client {addr[0]}:{addr[1]} "
                    f"-> 127.0.0.1:{target_port}",
                )

        session.send_to_backend(data, self)

    async def _open_backend_session(self, session: ClientBridgeSession) -> None:
        loop = asyncio.get_running_loop()
        try:
            await loop.create_datagram_endpoint(
                lambda: _BackendSessionProtocol(session, self),
                remote_addr=("127.0.0.1", session.target_port),
            )
        except Exception as exc:
            logger.debug("Failed to open backend UDP session to :%d: %s", session.target_port, exc)
            self.sessions.pop(session.client_addr, None)

    def _send_probe_ping(self) -> None:
        """Send internal RakNet and NetherNet probes to sync GUIDs and NetherNet IDs."""
        if not self.probe_transport:
            return
        if self.target:
            probe_pkt = (
                bytes([ID_UNCONNECTED_PING])
                + struct.pack(">Q", int(time.time() * 1000))
                + RAKNET_MAGIC
                + struct.pack(">Q", 0xBEEF19132)
            )
            try:
                self.probe_transport.sendto(probe_pkt, ("127.0.0.1", self.target.port))
            except Exception:
                pass

        nn_ports = self._get_online_nethernet_ports()
        if nn_ports:
            nn_probe = build_nethernet_discovery_probe()
            for nn_port in nn_ports:
                try:
                    self.probe_transport.sendto(nn_probe, ("127.0.0.1", nn_port))
                except Exception:
                    pass

    async def _housekeeping_loop(self) -> None:
        while True:
            await asyncio.sleep(2.0)
            if self.enabled:
                self._send_probe_ping()

            now = time.time()
            stale = [
                addr
                for addr, sess in self.sessions.items()
                if now - sess.last_active > self.session_timeout
            ]
            for addr in stale:
                sess = self.sessions.pop(addr, None)
                if sess:
                    sess.close()

            stale_nn = [
                key
                for key, sess in self.nethernet_sessions.items()
                if now - sess.last_active > self.session_timeout
            ]
            for key in stale_nn:
                sess = self.nethernet_sessions.pop(key, None)
                if sess:
                    sess.close()

    def get_status(self) -> dict[str, Any]:
        """Return broadcaster telemetry and configuration state for the Web GUI."""
        beacon_active = bool(
            self.enabled and self.bound_ok and (self.target is not None or bool(self.online_targets))
        )
        endpoints = [f"{str(addr[0]).replace('::ffff:', '')}:{addr[1]}" for addr in self.sessions.keys()]
        for addr, _ in self.nethernet_sessions.keys():
            ep = f"{str(addr[0]).replace('::ffff:', '')}:{addr[1]}"
            if ep not in endpoints:
                endpoints.append(ep)
        return {
            "enabled": self.enabled,
            "bound": self.bound_ok,
            "nethernet_bound": self.nethernet_bound_ok,
            "bind_port": self.bind_port,
            "nethernet_bind_port": self.nethernet_bind_port,
            "bind_error": self.bind_error,
            "lan_ip": self.lan_ip,
            "beacon_active": beacon_active,
            "target_instance_id": self.target.id if self.target else None,
            "target_display_name": self.target.display_name if self.target else None,
            "target_world_name": self.target.world_name if self.target else None,
            "target_port": self.target.port if self.target else None,
            "target_nethernet_port": self.target.nethernet_udp_port if self.target else None,
            "online_broadcast_count": len(self._get_online_nethernet_ports()),
            "motd_preview": self.build_current_motd() if self.target else None,
            "active_sessions": len(self.sessions) + len(self.nethernet_sessions),
            "client_endpoints": endpoints,
            "pings_received": self.pings_received,
            "pongs_sent": self.pongs_sent,
            "packets_to_server": self.packets_to_server,
            "packets_to_client": self.packets_to_client,
            "bytes_forwarded": self.bytes_forwarded,
            "last_ping_at": self.last_ping_at,
            "last_ping_from": self.last_ping_from,
        }
