"""
Automated Bootstrapper, Dependency Downloader & Self-Healing LAN Diagnostics (`/app/bootstrap.py`).

Responsibilities:
1. Auto-download and install official Mojang Bedrock Dedicated Server (`bds_bin/bedrock_server.exe`)
   when missing (e.g., fresh clone or zip download from GitHub).
2. Ensure Microsoft Visual C++ Runtime DLLs (`vcruntime140.dll`, `vcruntime140_1.dll`, `msvcp140.dll`, etc.)
   are present in `redist/`, `bds_bin/`, and every `instances/<id>/` folder, and automatically download/install
   `vc_redist.x64.exe` on Windows if system-wide VC++ Redistributable is missing.
3. Automatically configure Windows Defender Firewall rules (removing any accidental Block rules on
   `python.exe` or `bedrock_server.exe` and allowing UDP 7551, 19132-19165, 49152-65535, and TCP 8000
   across Domain, Private, and Public profiles).
4. Continuously self-test and self-correct LAN broadcasting over both NetherNet (`UDP 7551`) and
   RakNet (`UDP 19132`), verifying that every online server instance responds to real encrypted
   LAN discovery pings and auto-healing port bindings, PID socket detection, or hung processes.
"""

from __future__ import annotations

import asyncio
import ctypes
import hashlib
import hmac
import io
import logging
import os
from pathlib import Path
import re
import shutil
import socket
import struct
import subprocess
import sys
import time
from typing import TYPE_CHECKING, Any
import urllib.request
import zipfile

import psutil
from cryptography.hazmat.primitives import padding as sym_padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

if TYPE_CHECKING:
    from app.broadcaster import RakNetLANBroadcaster
    from app.manager import InstanceManager

logger = logging.getLogger("bedrock.bootstrap")

BASE_DIR = Path(__file__).resolve().parent.parent
BDS_BIN_DIR = BASE_DIR / "bds_bin"
REDIST_DIR = BASE_DIR / "redist"
INSTANCES_DIR = BASE_DIR / "instances"
DATA_DIR = BASE_DIR / "data"

DEFAULT_BDS_WIN_URL = (
    "https://www.minecraft.net/bedrockdedicatedserver/bin-win/bedrock-server-1.26.61.1.zip"
)
DEFAULT_BDS_LINUX_URL = (
    "https://www.minecraft.net/bedrockdedicatedserver/bin-linux/bedrock-server-1.26.61.1.zip"
)
VC_REDIST_X64_URL = "https://aka.ms/vs/17/release/vc_redist.x64.exe"

VC_RUNTIME_DLLS = (
    "vcruntime140.dll",
    "vcruntime140_1.dll",
    "vcruntime140_threads.dll",
    "msvcp140.dll",
    "msvcp140_1.dll",
    "msvcp140_2.dll",
    "msvcp140_atomic_wait.dll",
    "msvcp140_codecvt_ids.dll",
    "concrt140.dll",
    "vccorlib140.dll",
)

NETHERNET_KEY = hashlib.sha256(struct.pack("<Q", 0xDEADBEEF)).digest()
_NETHERNET_CIPHER = Cipher(algorithms.AES(NETHERNET_KEY), modes.ECB())

RAKNET_MAGIC = b"\x00\xff\xff\x00\xfe\xfe\xfe\xfe\xfd\xfd\xfd\xfd\x12\x34\x56\x78"
ID_UNCONNECTED_PING = 0x01
ID_UNCONNECTED_PONG = 0x1C


def build_nethernet_discovery_request(sender_id: int = 0xBEEF7551CAFE1913) -> bytes:
    """Build an encrypted 64-byte NetherNet `DiscoveryRequestPacket` (packet_id=0)."""
    plaintext = struct.pack("<HHQQ", 20, 0, sender_id, 0)
    chk = hmac.new(NETHERNET_KEY, plaintext, hashlib.sha256).digest()
    padder = sym_padding.PKCS7(128).padder()
    padded = padder.update(plaintext) + padder.finalize()
    encryptor = _NETHERNET_CIPHER.encryptor()
    return chk + encryptor.update(padded) + encryptor.finalize()


def decode_nethernet_discovery_response(pkt: bytes) -> dict[str, Any] | None:
    """Decrypt and parse a NetherNet `DiscoveryResponsePacket` (packet_id=1)."""
    if len(pkt) < 64 or (len(pkt) - 32) % 16 != 0:
        return None
    try:
        decryptor = _NETHERNET_CIPHER.decryptor()
        padded = decryptor.update(pkt[32:]) + decryptor.finalize()
        unpadder = sym_padding.PKCS7(128).unpadder()
        plain = unpadder.update(padded) + unpadder.finalize()
        if len(plain) < 24:
            return None
        length, pkt_id, server_net_id, _, hex_len = struct.unpack("<HHQQI", plain[:24])
        if pkt_id != 1:
            return None
        hex_ascii = plain[24 : 24 + hex_len].decode("ascii", errors="ignore")
        raw = bytes.fromhex(hex_ascii)
        # Parse server_name and world_name from binary payload:
        # byte 0: version (0x07), byte 1: server_name len, then server_name, then 4 bytes protocol,
        # then version_str len + version_str, then world_name len + world_name
        pos = 1
        srv_name = ""
        world_name = ""
        if len(raw) > pos:
            n_len = raw[pos]
            pos += 1
            srv_name = raw[pos : pos + n_len].decode("utf-8", errors="replace")
            pos += n_len + 1  # skip 1-byte flags/type field (0x22)
            if len(raw) > pos:
                v_len = raw[pos]
                pos += 1 + v_len  # skip version string (e.g. "1.26.61")
                if len(raw) > pos:
                    w_len = raw[pos]
                    pos += 1
                    world_name = raw[pos : pos + w_len].decode("utf-8", errors="replace")
        return {
            "server_network_id": server_net_id,
            "server_network_id_hex": f"0x{server_net_id:016x}",
            "server_name": srv_name,
            "world_name": world_name,
            "raw_payload_len": len(raw),
        }
    except Exception:
        return None


def probe_udp_port_for_nethernet(port: int, timeout: float = 0.25) -> dict[str, Any] | None:
    """Send a NetherNet DiscoveryRequest directly to `127.0.0.1:<port>` and check if it replies."""
    req = build_nethernet_discovery_request(sender_id=0x7551000000000000 | (port & 0xFFFF))
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.settimeout(timeout)
            s.sendto(req, ("127.0.0.1", port))
            data, _ = s.recvfrom(4096)
            return decode_nethernet_discovery_response(data)
    except Exception:
        return None


def discover_pid_nethernet_port(pid: int) -> int | None:
    """
    Find the active NetherNet UDP port for a `bedrock_server.exe` process by inspecting all
    UDP sockets owned by `pid` and probing them if multiple UDP sockets exist.
    """
    candidate_ports: list[int] = []
    try:
        proc = psutil.Process(pid)
        for conn in proc.net_connections(kind="udp"):
            if conn.laddr and conn.laddr.port:
                p = int(conn.laddr.port)
                # Prefer dual-stack IPv6 ('::') sockets first, as BDS binds [::]:<port>
                if str(conn.laddr.ip) in ("::", "0.0.0.0", ""):
                    if p not in candidate_ports:
                        candidate_ports.insert(0, p)
                elif p not in candidate_ports:
                    candidate_ports.append(p)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass

    # Fallback to system-wide psutil.net_connections if per-process query returned nothing
    if not candidate_ports:
        try:
            for conn in psutil.net_connections(kind="udp"):
                if conn.pid == pid and conn.laddr and conn.laddr.port:
                    p = int(conn.laddr.port)
                    if p not in candidate_ports:
                        candidate_ports.append(p)
        except Exception:
            pass

    if not candidate_ports:
        return None
    if len(candidate_ports) == 1:
        return candidate_ports[0]

    # Probe each candidate port to find the exact one responding to NetherNet discovery
    for p in candidate_ports:
        if probe_udp_port_for_nethernet(p, timeout=0.15) is not None:
            return p
    return candidate_ports[0]


# -----------------------------------------------------------------------------
# 1. Visual C++ Redistributable & Local DLL Provisioning
# -----------------------------------------------------------------------------


def is_vc_runtime_loadable() -> bool:
    """Check whether Windows can load `vcruntime140.dll`, `vcruntime140_1.dll`, and `msvcp140.dll`."""
    if os.name != "nt":
        return True
    REDIST_DIR.mkdir(parents=True, exist_ok=True)
    # First check if system or redist DLLs can be loaded
    for dll_name in ("vcruntime140.dll", "vcruntime140_1.dll", "msvcp140.dll"):
        try:
            ctypes.WinDLL(dll_name)
        except OSError:
            local_dll = REDIST_DIR / dll_name
            if local_dll.exists():
                try:
                    ctypes.WinDLL(str(local_dll))
                    continue
                except OSError:
                    pass
            return False
    return True


def sync_vc_dlls_to_directory(target_dir: Path) -> int:
    """
    Copy VC++ 140 runtime DLLs from `redist/`, `System32`, or Python's base directory into `target_dir`
    so `bedrock_server.exe` always resolves its required DLLs locally even without Admin install.
    """
    if os.name != "nt":
        return 0
    target_dir.mkdir(parents=True, exist_ok=True)
    REDIST_DIR.mkdir(parents=True, exist_ok=True)

    sys32 = Path(os.environ.get("SystemRoot", r"C:\Windows")) / "System32"
    py_dir = Path(sys.base_prefix)
    copied = 0

    for dll_name in VC_RUNTIME_DLLS:
        dest = target_dir / dll_name
        # Ensure redist/ has a copy if available in System32 or Python dir
        redist_copy = REDIST_DIR / dll_name
        if not redist_copy.exists():
            for src_dir in (sys32, py_dir):
                candidate = src_dir / dll_name
                if candidate.exists():
                    try:
                        shutil.copy2(candidate, redist_copy)
                        break
                    except Exception:
                        pass

        src_file = redist_copy if redist_copy.exists() else (sys32 / dll_name)
        if src_file.exists() and not dest.exists():
            try:
                shutil.copy2(src_file, dest)
                copied += 1
            except Exception:
                pass
    return copied


def ensure_vc_redist(auto_install: bool = True) -> dict[str, Any]:
    """
    Ensure Microsoft Visual C++ Runtime is available both locally in `redist/` + `bds_bin/`
    and system-wide via `vc_redist.x64.exe` if missing.
    """
    if os.name != "nt":
        return {"ok": True, "platform": sys.platform, "action": "none"}

    REDIST_DIR.mkdir(parents=True, exist_ok=True)
    sync_vc_dlls_to_directory(BDS_BIN_DIR)
    if INSTANCES_DIR.exists():
        for inst_dir in INSTANCES_DIR.iterdir():
            if inst_dir.is_dir():
                sync_vc_dlls_to_directory(inst_dir)

    sys32 = Path(os.environ.get("SystemRoot", r"C:\Windows")) / "System32"
    system_has_dlls = all(
        (sys32 / d).exists() for d in ("vcruntime140.dll", "vcruntime140_1.dll", "msvcp140.dll")
    )
    local_has_dlls = all(
        (REDIST_DIR / d).exists()
        for d in ("vcruntime140.dll", "vcruntime140_1.dll", "msvcp140.dll")
    )

    if system_has_dlls or local_has_dlls:
        return {
            "ok": True,
            "system_installed": system_has_dlls,
            "bundled_in_redist": local_has_dlls,
            "action": "verified",
        }

    if not auto_install:
        return {
            "ok": False,
            "system_installed": False,
            "bundled_in_redist": False,
            "action": "missing",
        }

    # Download and install Microsoft Visual C++ Redistributable x64
    installer_path = REDIST_DIR / "vc_redist.x64.exe"
    try:
        if not installer_path.exists() or installer_path.stat().st_size < 1_000_000:
            logger.info("Downloading Microsoft Visual C++ Redistributable (vc_redist.x64.exe)...")
            req = urllib.request.Request(
                VC_REDIST_X64_URL,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
            )
            with urllib.request.urlopen(req, timeout=60) as resp:
                installer_path.write_bytes(resp.read())

        logger.info("Installing Microsoft Visual C++ Redistributable...")
        subprocess.run(
            [str(installer_path), "/install", "/passive", "/norestart"],
            check=False,
            timeout=180,
        )
        sync_vc_dlls_to_directory(BDS_BIN_DIR)
        if INSTANCES_DIR.exists():
            for inst_dir in INSTANCES_DIR.iterdir():
                if inst_dir.is_dir():
                    sync_vc_dlls_to_directory(inst_dir)
        return {
            "ok": is_vc_runtime_loadable(),
            "system_installed": True,
            "bundled_in_redist": True,
            "action": "downloaded_and_installed_vc_redist",
        }
    except Exception as exc:
        logger.warning("VC++ Redistributable auto-install failed: %s", exc)
        return {"ok": is_vc_runtime_loadable(), "error": str(exc), "action": "install_failed"}


# -----------------------------------------------------------------------------
# 2. Official Mojang Bedrock Dedicated Server Auto-Downloader
# -----------------------------------------------------------------------------


def _flatten_nested_bds_dir() -> bool:
    """
    If a user extracted `bedrock-server-*.zip` into `bds_bin/bedrock-server-1.26.x/bedrock_server.exe`,
    automatically move its contents up into `bds_bin/`.
    """
    exe_name = "bedrock_server.exe" if os.name == "nt" else "bedrock_server"
    if (BDS_BIN_DIR / exe_name).exists():
        return True
    if not BDS_BIN_DIR.exists():
        return False

    for child in BDS_BIN_DIR.iterdir():
        if child.is_dir() and (child / exe_name).exists():
            logger.info("Flattening nested BDS directory %s into %s", child, BDS_BIN_DIR)
            for item in child.iterdir():
                dest = BDS_BIN_DIR / item.name
                if not dest.exists():
                    shutil.move(str(item), str(dest))
            try:
                child.rmdir()
            except Exception:
                pass
            return (BDS_BIN_DIR / exe_name).exists()
    return False


def _find_local_bds_zip() -> Path | None:
    """Check if `bedrock-server-*.zip` was already downloaded to the workspace or ~/Downloads."""
    search_dirs = [BASE_DIR, BDS_BIN_DIR, Path.home() / "Downloads"]
    for d in search_dirs:
        if not d.exists():
            continue
        matches = sorted(
            d.glob("bedrock-server-*.zip"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        for m in matches:
            if m.stat().st_size > 10_000_000:
                return m
    return None


def _resolve_bds_download_url() -> str:
    """Return the official Mojang Bedrock Dedicated Server download URL."""
    default_url = DEFAULT_BDS_WIN_URL if os.name == "nt" else DEFAULT_BDS_LINUX_URL
    return os.environ.get("BDS_DOWNLOAD_URL", default_url)


def ensure_bds_binary(auto_download: bool = True) -> dict[str, Any]:
    """
    Ensure `bds_bin/bedrock_server.exe` (or `bds_bin/bedrock_server` on Linux) exists.
    If missing, automatically extracts a local `bedrock-server-*.zip` or downloads it from Mojang.
    """
    exe_name = "bedrock_server.exe" if os.name == "nt" else "bedrock_server"
    BDS_BIN_DIR.mkdir(parents=True, exist_ok=True)
    _flatten_nested_bds_dir()

    exe_path = BDS_BIN_DIR / exe_name
    if exe_path.exists() and exe_path.stat().st_size > 5_000_000:
        sync_vc_dlls_to_directory(BDS_BIN_DIR)
        return {"ok": True, "path": str(exe_path), "action": "verified"}

    if not auto_download:
        return {"ok": False, "path": str(exe_path), "action": "missing"}

    # Check if a local zip already exists in workspace or Downloads
    local_zip = _find_local_bds_zip()
    if local_zip is not None:
        logger.info("Found local BDS archive %s; extracting into %s...", local_zip, BDS_BIN_DIR)
        try:
            with zipfile.ZipFile(local_zip, "r") as zf:
                zf.extractall(BDS_BIN_DIR)
            _flatten_nested_bds_dir()
            if os.name != "nt" and exe_path.exists():
                exe_path.chmod(0o755)
            sync_vc_dlls_to_directory(BDS_BIN_DIR)
            if exe_path.exists():
                return {
                    "ok": True,
                    "path": str(exe_path),
                    "source": str(local_zip),
                    "action": "extracted_local_zip",
                }
        except Exception as exc:
            logger.warning("Failed to extract local zip %s: %s", local_zip, exc)

    # Download directly from Mojang
    url = _resolve_bds_download_url()
    zip_target = BDS_BIN_DIR / "bedrock-server-download.zip"
    logger.info("Downloading official Mojang Bedrock Dedicated Server from %s ...", url)
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "*/*",
            },
        )
        with urllib.request.urlopen(req, timeout=180) as resp:
            total = int(resp.headers.get("Content-Length", "0") or "0")
            downloaded = 0
            last_pct = -10
            with open(zip_target, "wb") as f:
                while True:
                    chunk = resp.read(256 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total > 0:
                        pct = int((downloaded * 100) / total)
                        if pct >= last_pct + 10:
                            last_pct = pct
                            logger.info(
                                "Downloading BDS binary: %d%% (%.1f / %.1f MB)",
                                pct,
                                downloaded / (1024 * 1024),
                                total / (1024 * 1024),
                            )

        logger.info("Extracting Bedrock Dedicated Server into %s ...", BDS_BIN_DIR)
        with zipfile.ZipFile(zip_target, "r") as zf:
            zf.extractall(BDS_BIN_DIR)
        try:
            zip_target.unlink()
        except Exception:
            pass

        _flatten_nested_bds_dir()
        if os.name != "nt" and exe_path.exists():
            exe_path.chmod(0o755)
        sync_vc_dlls_to_directory(BDS_BIN_DIR)

        return {
            "ok": exe_path.exists(),
            "path": str(exe_path),
            "url": url,
            "action": "downloaded_from_mojang",
        }
    except Exception as exc:
        logger.error("Failed to download official BDS binary from %s: %s", url, exc)
        return {"ok": False, "url": url, "error": str(exc), "action": "download_failed"}


# -----------------------------------------------------------------------------
# 3. Windows Defender Firewall Auto-Configuration
# -----------------------------------------------------------------------------


def check_windows_firewall_status() -> dict[str, Any]:
    """
    Check whether Windows Firewall allows inbound LAN traffic for BedrockNexus (`BedrockNexus_LAN_UDP`
    or enabled `Allow` rules for `python.exe` and `bedrock_server.exe` with no `Block` rules).
    """
    if os.name != "nt":
        return {"ok": True, "platform": sys.platform, "rule_present": True}
    try:
        res = subprocess.run(
            [
                "netsh",
                "advfirewall",
                "firewall",
                "show",
                "rule",
                "name=BedrockNexus_LAN_UDP",
            ],
            capture_output=True,
            text=True,
            timeout=8,
        )
        if res.returncode == 0 and "BedrockNexus_LAN_UDP" in res.stdout:
            return {"ok": True, "rule_present": True, "mode": "port_rule"}

        # Check application rules for python.exe and bedrock_server.exe
        ps_check = (
            "$ErrorActionPreference = 'SilentlyContinue'; "
            "$filters = Get-NetFirewallApplicationFilter | Where-Object { $_.Program -match 'bedrock_server\\.exe|python\\.exe' }; "
            "$rules = $filters | ForEach-Object { Get-NetFirewallRule -AssociatedNetFirewallApplicationFilter $_ } | Where-Object { $_.Enabled -eq 'True' }; "
            "$blocks = $rules | Where-Object { $_.Action -eq 'Block' }; "
            "$allows = $rules | Where-Object { $_.Action -eq 'Allow' }; "
            "if ($blocks) { Write-Output 'BLOCKED' } elseif (($allows | Measure-Object).Count -ge 2) { Write-Output 'ALLOWED' } else { Write-Output 'MISSING' }"
        )
        res2 = subprocess.run(
            ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps_check],
            capture_output=True,
            text=True,
            timeout=10,
        )
        out = (res2.stdout or "").strip()
        if "ALLOWED" in out and "BLOCKED" not in out:
            return {"ok": True, "rule_present": True, "mode": "app_rules"}
        return {"ok": False, "rule_present": False, "mode": out or "missing"}
    except Exception as exc:
        return {"ok": False, "rule_present": False, "error": str(exc)}


def ensure_windows_firewall_rules(allow_elevation_prompt: bool = False) -> dict[str, Any]:
    """
    Ensure Windows Defender Firewall allows UDP 7551 (NetherNet), UDP 19132-19165 (RakNet/BDS),
    UDP 49152-65535 (NetherNet WebRTC ephemeral ports), TCP 8000 (Web GUI), `python.exe`, and
    `bedrock_server.exe` across ALL profiles (Domain, Private, Public), and removes any Block rules
    that Windows Firewall may have created on `python.exe` or `bedrock_server.exe`.
    """
    if os.name != "nt":
        return {"ok": True, "action": "not_windows"}

    python_exe = str(Path(sys.executable).resolve())
    bds_exe = str((BDS_BIN_DIR / "bedrock_server.exe").resolve())

    ps_lines = [
        "$ErrorActionPreference = 'SilentlyContinue'",
        "Get-NetFirewallApplicationFilter | Where-Object { $_.Program -match 'bedrock_server\\.exe|python\\.exe' } | Get-NetFirewallRule | Where-Object { $_.Action -eq 'Block' } | Remove-NetFirewallRule",
        'netsh advfirewall firewall delete rule name="BedrockNexus_LAN_UDP" | Out-Null',
        'netsh advfirewall firewall add rule name="BedrockNexus_LAN_UDP" dir=in action=allow protocol=UDP localport=7551,19132-19165,20132-20165,49152-65535 profile=any | Out-Null',
        'netsh advfirewall firewall add rule name="BedrockNexus_LAN_UDP" dir=out action=allow protocol=UDP localport=any profile=any | Out-Null',
        'netsh advfirewall firewall delete rule name="BedrockNexus_Web_TCP" | Out-Null',
        'netsh advfirewall firewall add rule name="BedrockNexus_Web_TCP" dir=in action=allow protocol=TCP localport=8000 profile=any | Out-Null',
        'netsh advfirewall firewall delete rule name="BedrockNexus_Python" | Out-Null',
        f'netsh advfirewall firewall add rule name="BedrockNexus_Python" dir=in action=allow program="{python_exe}" profile=any | Out-Null',
        'netsh advfirewall firewall delete rule name="BedrockNexus_BDS" | Out-Null',
        f'netsh advfirewall firewall add rule name="BedrockNexus_BDS" dir=in action=allow program="{bds_exe}" profile=any | Out-Null',
    ]
    if INSTANCES_DIR.exists():
        for inst_dir in INSTANCES_DIR.iterdir():
            inst_exe = inst_dir / "bedrock_server.exe"
            if inst_exe.exists():
                ps_lines.append(
                    f'netsh advfirewall firewall add rule name="BedrockNexus_BDS_{inst_dir.name}" dir=in action=allow program="{inst_exe.resolve()}" profile=any | Out-Null'
                )

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    script_path = DATA_DIR / "configure_firewall.ps1"
    script_path.write_text("\n".join(ps_lines) + "\n", encoding="utf-8")

    # First try running directly (succeeds if already running as Administrator)
    try:
        subprocess.run(
            ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script_path)],
            capture_output=True,
            text=True,
            timeout=15,
        )
    except Exception:
        pass

    status = check_windows_firewall_status()
    if status.get("rule_present") and status.get("mode") == "port_rule":
        return {"ok": True, "rule_present": True, "action": "configured_direct"}

    if allow_elevation_prompt and not status.get("rule_present"):
        try:
            elevate_cmd = (
                f"Start-Process powershell -Verb RunAs -WindowStyle Hidden -Wait "
                f"-ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-File','{script_path}'"
            )
            subprocess.run(
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", elevate_cmd],
                capture_output=True,
                text=True,
                timeout=45,
            )
        except Exception as exc:
            logger.debug("Elevated firewall configuration skipped or declined: %s", exc)

    final_status = check_windows_firewall_status()
    return {
        "ok": bool(final_status.get("rule_present")),
        "rule_present": bool(final_status.get("rule_present")),
        "action": "configured" if final_status.get("rule_present") else "needs_admin_for_firewall",
    }


# -----------------------------------------------------------------------------
# 4. Synthetic Simulator World Stub Cleanup
# -----------------------------------------------------------------------------


def clean_simulator_world_stubs() -> int:
    """
    If `ensure_world_files` previously wrote a synthetic `BDS-LEVELDB-MANIFEST-V1:` manifest
    or synthetic JSON `level.dat` before `bds_bin/bedrock_server.exe` was downloaded, remove
    those synthetic stub files so official `bedrock_server.exe` generates a clean vanilla world.
    """
    cleaned = 0
    if not INSTANCES_DIR.exists():
        return 0

    for world_dir in INSTANCES_DIR.glob("*/worlds/*"):
        if not world_dir.is_dir():
            continue
        is_stub = False
        level_dat = world_dir / "level.dat"
        manifest = world_dir / "db" / "MANIFEST-000001"

        try:
            if level_dat.exists() and level_dat.stat().st_size < 512:
                raw_dat = level_dat.read_bytes()
                if b'{"LevelName"' in raw_dat:
                    is_stub = True
            if manifest.exists() and manifest.stat().st_size < 512:
                raw_man = manifest.read_bytes()
                if b"BDS-LEVELDB-MANIFEST" in raw_man or b"BEDROCK-LEVELDB-MANIFEST" in raw_man:
                    is_stub = True

            if is_stub:
                shutil.rmtree(world_dir / "db", ignore_errors=True)
                level_dat.unlink(missing_ok=True)
                (world_dir / "levelname.txt").unlink(missing_ok=True)
                cleaned += 1
                logger.info("Cleaned synthetic simulator world stub in %s", world_dir)
        except Exception:
            pass
    return cleaned


def run_startup_bootstrap(allow_firewall_elevation: bool = False) -> dict[str, Any]:
    """
    Execute full startup provisioning:
    1. Ensure VC++ runtime DLLs & redistributable.
    2. Ensure official Mojang `bedrock_server` binary in `bds_bin/`.
    3. Clean any synthetic simulator world stubs.
    4. Ensure Windows Defender Firewall rules.
    """
    vc_res = ensure_vc_redist(auto_install=True)
    bds_res = ensure_bds_binary(auto_download=True)
    stubs_cleaned = clean_simulator_world_stubs()
    fw_res = ensure_windows_firewall_rules(allow_elevation_prompt=allow_firewall_elevation)
    return {
        "vc_runtime": vc_res,
        "bds_binary": bds_res,
        "stubs_cleaned": stubs_cleaned,
        "firewall": fw_res,
    }


# -----------------------------------------------------------------------------
# 5. Live Self-Test & Self-Healing Coordinator
# -----------------------------------------------------------------------------


class SelfHealingCoordinator:
    """
    Continuously tests and auto-heals the multi-server environment and LAN broadcaster:
    - Verifies `bds_bin/bedrock_server.exe` and VC++ runtime DLLs.
    - Verifies UDP 7551 (NetherNet) and UDP 19132 (RakNet) proxy sockets are bound.
    - Reclaims UDP 7551 if an orphaned or prematurely started `bedrock_server.exe` grabbed it.
    - Sends real encrypted NetherNet (`UDP 7551`) and RakNet (`UDP 19132`) LAN discovery pings
      to verify that every online instance responds with its proper Server Name and World Name.
    - Auto-repairs PID socket detection, re-links instance binaries/DLLs, or restarts unresponsive
      instances if a running server fails the broadcast self-test.
    """

    def __init__(
        self,
        manager: "InstanceManager",
        broadcaster: "RakNetLANBroadcaster",
    ) -> None:
        self.manager = manager
        self.broadcaster = broadcaster
        self.last_report: dict[str, Any] = {
            "status": "initializing",
            "last_checked_at": None,
            "checks": [],
            "heals_performed": [],
            "nethernet_responders": [],
            "raknet_pong_ok": False,
        }
        self._lock = asyncio.Lock()
        self._fail_streaks: dict[str, int] = {}

    async def run_check_and_heal(self, force_deep: bool = False) -> dict[str, Any]:
        """Run a complete diagnostic & auto-healing pass and return the structured report."""
        async with self._lock:
            checks: list[dict[str, Any]] = []
            heals: list[str] = []

            # 1. Verify VC++ Runtime & Official BDS Binary
            exe_name = "bedrock_server.exe" if os.name == "nt" else "bedrock_server"
            bds_exe = BDS_BIN_DIR / exe_name
            if not bds_exe.exists() or not is_vc_runtime_loadable():
                boot_res = await asyncio.to_thread(run_startup_bootstrap, False)
                heals.append("Auto-provisioned BDS binary and VC++ runtime DLLs")
                checks.append(
                    {
                        "name": "BDS Binary & VC++ Runtime",
                        "ok": bool(bds_exe.exists() and is_vc_runtime_loadable()),
                        "detail": f"Provisioned ({boot_res['bds_binary'].get('action')})",
                    }
                )
            else:
                checks.append(
                    {
                        "name": "BDS Binary & VC++ Runtime",
                        "ok": True,
                        "detail": f"Official binary & VC++ DLLs ready ({bds_exe.name})",
                    }
                )

            # 2. Verify Proxy Socket Bindings (UDP 7551 NetherNet & UDP 19132 RakNet)
            if not self.broadcaster.bound_ok or not self.broadcaster.nethernet_bound_ok:
                healed_bind = await self._heal_proxy_socket_bindings(heals)
                checks.append(
                    {
                        "name": "LAN Proxy Sockets (UDP 19132 & 7551)",
                        "ok": healed_bind,
                        "detail": (
                            f"RakNet :19132={'BOUND' if self.broadcaster.bound_ok else 'ERR'}, "
                            f"NetherNet :7551={'BOUND' if self.broadcaster.nethernet_bound_ok else 'ERR'}"
                        ),
                    }
                )
            else:
                checks.append(
                    {
                        "name": "LAN Proxy Sockets (UDP 19132 & 7551)",
                        "ok": True,
                        "detail": "Both UDP :19132 (RakNet) and UDP [::]:7551 (NetherNet) bound",
                    }
                )

            # 3. Inspect Running Instances & Upgrade Simulator / Fix Port 7551 Stealing
            instances = self.manager.list_instances()
            online_instances = [i for i in instances if i.get("status") == "online"]

            for inst in list(online_instances):
                iid = inst["id"]
                # If an instance started in fallback simulator mode before bds_bin finished downloading,
                # restart it now so it runs the real official BDS binary!
                if inst.get("engine_mode") == "Built-in BDS Engine" and self.manager.has_official_bds():
                    heals.append(
                        f"Switched '{inst['display_name']}' from Fallback Simulator to Official BDS Binary"
                    )
                    await asyncio.to_thread(clean_simulator_world_stubs)
                    await self.manager.restart_instance(iid)
                    await asyncio.sleep(2.5)
                # If a managed BDS instance bound port 7551 directly (because proxy wasn't bound earlier),
                # reclaim 7551 for the proxy and restart the instance so it binds an ephemeral port
                elif inst.get("nethernet_udp_port") == 7551:
                    heals.append(
                        f"Reclaimed UDP 7551 from '{inst['display_name']}' so multi-server proxy can route all instances"
                    )
                    await self.manager.stop_instance(iid)
                    await self.broadcaster.ensure_sockets_bound()
                    await self.manager.start_instance(iid)
                    await asyncio.sleep(2.5)

            # Refresh instance list and sync broadcaster
            instances = self.manager.list_instances()
            online_instances = [i for i in instances if i.get("status") == "online"]
            self.broadcaster.sync_online_instances(instances)

            # Ensure primary target points to an online instance if one exists
            if online_instances and (
                self.broadcaster.target is None
                or self.broadcaster.target.id not in {i["id"] for i in online_instances}
            ):
                self.broadcaster.set_target_instance(online_instances[0])
                self.manager.set_setting("broadcaster_instance_id", online_instances[0]["id"])
                heals.append(
                    f"Auto-selected online instance '{online_instances[0]['display_name']}' as active LAN target"
                )

            # 4. Live End-to-End NetherNet (UDP 7551) Broadcast Verification
            nethernet_responders: list[dict[str, Any]] = []
            if self.broadcaster.enabled and online_instances:
                nethernet_responders = await asyncio.to_thread(
                    self._probe_proxy_nethernet,
                    len(online_instances),
                )
                responder_names = {r.get("server_name") for r in nethernet_responders}
                missing = [
                    inst
                    for inst in online_instances
                    if inst["display_name"] not in responder_names
                ]

                if missing:
                    # Attempt immediate self-correction: re-probe PIDs for NetherNet UDP ports
                    for inst in missing:
                        iid = inst["id"]
                        pid = inst.get("pid")
                        if pid:
                            true_port = await asyncio.to_thread(discover_pid_nethernet_port, int(pid))
                            if true_port and true_port != 7551:
                                inst["nethernet_udp_port"] = true_port
                    self.broadcaster.sync_online_instances(instances)
                    if self.broadcaster.target:
                        for inst in online_instances:
                            if inst["id"] == self.broadcaster.target.id:
                                self.broadcaster.set_target_instance(inst)

                    # Re-test UDP 7551 after refreshing ports
                    nethernet_responders = await asyncio.to_thread(
                        self._probe_proxy_nethernet,
                        len(online_instances),
                    )
                    responder_names = {r.get("server_name") for r in nethernet_responders}
                    still_missing = [
                        inst
                        for inst in online_instances
                        if inst["display_name"] not in responder_names
                    ]

                    for inst in still_missing:
                        iid = inst["id"]
                        uptime = float(inst.get("uptime_seconds", 0) or 0)
                        if uptime > 8.0 or force_deep:
                            self._fail_streaks[iid] = self._fail_streaks.get(iid, 0) + 1
                            if self._fail_streaks[iid] >= 2 or force_deep:
                                self._fail_streaks[iid] = 0
                                heals.append(
                                    f"Auto-restarted '{inst['display_name']}' after failing NetherNet UDP 7551 broadcast check"
                                )
                                await asyncio.to_thread(
                                    sync_vc_dlls_to_directory,
                                    self.manager.instance_dir(iid),
                                )
                                await asyncio.to_thread(clean_simulator_world_stubs)
                                await self.manager.restart_instance(iid)
                                await asyncio.sleep(2.5)
                                instances = self.manager.list_instances()
                                online_instances = [
                                    i for i in instances if i.get("status") == "online"
                                ]
                                self.broadcaster.sync_online_instances(instances)
                                nethernet_responders = await asyncio.to_thread(
                                    self._probe_proxy_nethernet,
                                    len(online_instances),
                                )
                else:
                    for inst in online_instances:
                        self._fail_streaks[inst["id"]] = 0

                all_nn_ok = len(nethernet_responders) >= len(online_instances)
                checks.append(
                    {
                        "name": "NetherNet LAN Discovery Self-Test (UDP 7551)",
                        "ok": all_nn_ok,
                        "detail": (
                            f"{len(nethernet_responders)}/{len(online_instances)} online server(s) responding on UDP 7551: "
                            + ", ".join(
                                f"{r['server_name']} ({r['world_name']})"
                                for r in nethernet_responders
                            )
                            if nethernet_responders
                            else "Waiting for server NetherNet port initialization..."
                        ),
                    }
                )
            else:
                checks.append(
                    {
                        "name": "NetherNet LAN Discovery Self-Test (UDP 7551)",
                        "ok": True,
                        "detail": "Standby (no instances currently online)",
                    }
                )

            # 5. Live End-to-End RakNet (UDP 19132) Broadcast Verification
            raknet_ok = False
            raknet_detail = "Standby"
            if self.broadcaster.enabled and self.broadcaster.target:
                rak_res = await asyncio.to_thread(self._probe_proxy_raknet)
                raknet_ok = bool(rak_res.get("ok"))
                raknet_detail = str(rak_res.get("detail", ""))
                checks.append(
                    {
                        "name": "RakNet Console Bridge Self-Test (UDP 19132)",
                        "ok": raknet_ok,
                        "detail": raknet_detail,
                    }
                )

            # 6. Windows Defender Firewall Check
            fw_status = await asyncio.to_thread(check_windows_firewall_status)
            if not fw_status.get("rule_present") and force_deep:
                fw_status = await asyncio.to_thread(ensure_windows_firewall_rules, True)
                if fw_status.get("rule_present"):
                    heals.append("Configured Windows Defender Firewall inbound UDP/TCP rules")
            checks.append(
                {
                    "name": "Windows Firewall LAN Rules",
                    "ok": bool(fw_status.get("rule_present", True)),
                    "detail": (
                        "Inbound UDP 7551 & 19132 allowed across all profiles"
                        if fw_status.get("rule_present", True)
                        else "Click 'Auto-Heal & Repair' to authorize Windows Firewall rule (UAC)"
                    ),
                }
            )

            overall_ok = all(c["ok"] for c in checks)
            self.last_report = {
                "status": "healthy" if overall_ok else "healing",
                "ok": overall_ok,
                "last_checked_at": time.time(),
                "checks": checks,
                "heals_performed": heals,
                "nethernet_responders": nethernet_responders,
                "raknet_pong_ok": raknet_ok,
            }
            return self.last_report

    async def _heal_proxy_socket_bindings(self, heals: list[str]) -> bool:
        """
        If UDP 7551 or UDP 19132 failed to bind, check if an orphaned `bedrock_server.exe`
        is holding the port, terminate it, and re-bind the proxy sockets.
        """
        managed_pids = {
            rt.process.pid
            for rt in self.manager.runtimes.values()
            if rt.process and rt.process.returncode is None
        }
        try:
            for conn in psutil.net_connections(kind="udp"):
                if conn.laddr and conn.laddr.port in (7551, 19132) and conn.pid:
                    if conn.pid == os.getpid():
                        continue
                    try:
                        proc = psutil.Process(conn.pid)
                        pname = proc.name().lower()
                        if "bedrock_server" in pname and conn.pid not in managed_pids:
                            proc.kill()
                            heals.append(
                                f"Terminated orphaned bedrock_server.exe (PID {conn.pid}) holding UDP :{conn.laddr.port}"
                            )
                    except Exception:
                        pass
        except Exception:
            pass

        await self.broadcaster.ensure_sockets_bound()
        if self.broadcaster.bound_ok and self.broadcaster.nethernet_bound_ok:
            heals.append("Bound RakNet (:19132) and NetherNet (:7551) proxy listeners")
            return True
        return False

    def _probe_proxy_nethernet(self, expected_count: int) -> list[dict[str, Any]]:
        """
        Send an encrypted NetherNet DiscoveryRequest to the proxy on UDP 7551 and collect
        all DiscoveryResponse packets returned by the online BDS instances.
        """
        req = build_nethernet_discovery_request(sender_id=0x75515E1F7E570001)
        responders: dict[int, dict[str, Any]] = {}
        target_host = self.broadcaster.lan_ip or "127.0.0.1"

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.settimeout(0.35)
                for dest_ip in ("127.0.0.1", target_host):
                    try:
                        s.sendto(req, (dest_ip, self.broadcaster.nethernet_bind_port))
                    except Exception:
                        pass
                deadline = time.time() + 0.45
                while time.time() < deadline and len(responders) < max(1, expected_count):
                    try:
                        data, _ = s.recvfrom(4096)
                        decoded = decode_nethernet_discovery_response(data)
                        if decoded and decoded["server_network_id"] not in responders:
                            responders[decoded["server_network_id"]] = decoded
                    except socket.timeout:
                        break
                    except Exception:
                        break
        except Exception:
            pass

        return list(responders.values())

    def _probe_proxy_raknet(self) -> dict[str, Any]:
        """Send a RakNet `ID_UNCONNECTED_PING` (0x01) to UDP 19132 and verify the Pong response."""
        pkt = (
            bytes([ID_UNCONNECTED_PING])
            + struct.pack(">Q", int(time.time() * 1000))
            + RAKNET_MAGIC
            + struct.pack(">Q", 0x191325E1F7E57)
        )
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.settimeout(0.5)
                s.sendto(pkt, ("127.0.0.1", self.broadcaster.bind_port))
                data, _ = s.recvfrom(2048)
                if len(data) > 35 and data[0] == ID_UNCONNECTED_PONG:
                    motd_len = struct.unpack(">H", data[33:35])[0]
                    motd = data[35 : 35 + motd_len].decode("utf-8", errors="replace")
                    parts = motd.split(";")
                    srv_name = parts[1] if len(parts) > 1 else "Unknown"
                    world_name = parts[7] if len(parts) > 7 else "Unknown"
                    return {
                        "ok": True,
                        "detail": f"Pong OK: '{srv_name}' ({world_name})",
                    }
        except Exception as exc:
            return {"ok": False, "detail": f"RakNet probe failed: {exc}"}
        return {"ok": False, "detail": "Invalid RakNet Pong"}


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    allow_uac = "--elevate-firewall" in sys.argv
    result = run_startup_bootstrap(allow_firewall_elevation=allow_uac)
    logger.info("Startup bootstrap complete: %s", result)
