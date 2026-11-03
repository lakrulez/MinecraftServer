"""
FastAPI Backend, REST Endpoints & WebSocket Telemetry/Console Hub (`/app/main.py`).
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import logging
from pathlib import Path
import socket
import struct
import time
from typing import Any

from fastapi import FastAPI, File, HTTPException, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
import psutil
from pydantic import BaseModel, Field

from app.broadcaster import (
    ID_UNCONNECTED_PING,
    RAKNET_MAGIC,
    RakNetLANBroadcaster,
    parse_unconnected_pong,
)
from app.manager import BACKUPS_DIR, InstanceManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("bedrock.main")

STATIC_DIR = Path(__file__).resolve().parent / "static"


class InstanceCreateRequest(BaseModel):
    display_name: str = Field(..., min_length=1, max_length=80)
    world_name: str = Field(default="BedrockLevel", min_length=1, max_length=80)
    seed: str = Field(default="", max_length=64)
    difficulty: str = Field(default="normal")
    gamemode: str = Field(default="survival")
    max_players: int = Field(default=10, ge=1, le=200)


class InstanceUpdateRequest(BaseModel):
    display_name: str | None = None
    world_name: str | None = None
    seed: str | None = None
    difficulty: str | None = None
    gamemode: str | None = None
    max_players: int | None = Field(default=None, ge=1, le=200)


class ConsoleCommandRequest(BaseModel):
    command: str = Field(..., min_length=1, max_length=500)


class BroadcasterUpdateRequest(BaseModel):
    enabled: bool | None = None
    target_instance_id: str | None = None


class WebSocketHub:
    """Manages active browser WebSocket connections for live telemetry and console streaming."""

    def __init__(self) -> None:
        self.clients: set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self.clients.add(ws)

    def disconnect(self, ws: WebSocket) -> None:
        self.clients.discard(ws)

    async def broadcast_json(self, payload: dict[str, Any]) -> None:
        if not self.clients:
            return
        dead: list[WebSocket] = []
        for ws in list(self.clients):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)


ws_hub = WebSocketHub()


async def _on_console_line(instance_id: str, entry: dict[str, Any]) -> None:
    await ws_hub.broadcast_json(
        {
            "type": "console_line",
            "instance_id": instance_id,
            "entry": entry,
        }
    )


async def _on_state_change() -> None:
    _sync_broadcaster_target_metadata()
    await _broadcast_telemetry_snapshot()


manager = InstanceManager(
    console_Push_callback=_on_console_line,
    state_change_callback=_on_state_change,
)

broadcaster = RakNetLANBroadcaster(
    bind_host="0.0.0.0",
    bind_port=19132,
    event_callback=lambda inst_id, msg: manager.append_log_sync(inst_id, msg),
)


def _sync_broadcaster_target_metadata() -> None:
    """Keep the broadcaster's online instances and primary target in sync with the manager."""
    instances = manager.list_instances()
    broadcaster.sync_online_instances(instances)

    saved_id = (
        broadcaster.target.id
        if broadcaster.target
        else manager.get_setting("broadcaster_instance_id", "")
    )
    online_instances = [i for i in instances if i.get("status") == "online"]

    if saved_id:
        inst = next((i for i in instances if i["id"] == saved_id), None)
        if inst:
            # If the saved target is offline while another instance is online, auto-switch to the online one
            if inst.get("status") != "online" and online_instances:
                broadcaster.set_target_instance(online_instances[0])
                manager.set_setting("broadcaster_instance_id", online_instances[0]["id"])
            else:
                broadcaster.set_target_instance(inst)
            return

    if online_instances:
        broadcaster.set_target_instance(online_instances[0])
        manager.set_setting("broadcaster_instance_id", online_instances[0]["id"])
    elif instances:
        broadcaster.set_target_instance(instances[0])
        manager.set_setting("broadcaster_instance_id", instances[0]["id"])
    else:
        broadcaster.set_target_instance(None)


def _get_host_metrics() -> dict[str, Any]:
    vm = psutil.virtual_memory()
    return {
        "cpu_percent": round(psutil.cpu_percent(interval=None), 1),
        "ram_percent": round(vm.percent, 1),
        "ram_used_gb": round(vm.used / (1024**3), 2),
        "ram_total_gb": round(vm.total / (1024**3), 2),
    }


async def _broadcast_telemetry_snapshot() -> None:
    _sync_broadcaster_target_metadata()
    if not ws_hub.clients:
        return
    await ws_hub.broadcast_json(
        {
            "type": "telemetry",
            "instances": manager.list_instances(),
            "broadcaster": broadcaster.get_status(),
            "host": _get_host_metrics(),
        }
    )


async def _telemetry_loop() -> None:
    while True:
        await asyncio.sleep(1.5)
        try:
            await _broadcast_telemetry_snapshot()
        except Exception as exc:
            logger.debug("Telemetry broadcast error: %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Restore broadcaster configuration from SQLite
    enabled_setting = manager.get_setting("broadcaster_enabled", "true").lower() == "true"
    broadcaster.enabled = enabled_setting
    _sync_broadcaster_target_metadata()
    await broadcaster.start()

    telemetry_task = asyncio.create_task(_telemetry_loop())
    try:
        yield
    finally:
        telemetry_task.cancel()
        await broadcaster.stop()
        await manager.stop_all()


app = FastAPI(
    title="Bedrock Multi-Server Manager & RakNet LAN Proxy",
    version="1.0.0",
    lifespan=lifespan,
)

STATIC_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", response_class=HTMLResponse)
async def serve_dashboard() -> HTMLResponse:
    index_file = STATIC_DIR / "index.html"
    if not index_file.exists():
        raise HTTPException(status_code=404, detail="Dashboard index.html not found")
    return HTMLResponse(index_file.read_text(encoding="utf-8"))


@app.get("/api/state")
async def get_full_state() -> dict[str, Any]:
    _sync_broadcaster_target_metadata()
    return {
        "instances": manager.list_instances(),
        "broadcaster": broadcaster.get_status(),
        "backups": manager.list_backups(),
        "host": _get_host_metrics(),
    }


# -----------------------------------------------------------------------------
# Instance Management Endpoints
# -----------------------------------------------------------------------------


@app.get("/api/instances")
async def api_list_instances() -> dict[str, Any]:
    return {"instances": manager.list_instances()}


@app.post("/api/instances")
async def api_create_instance(req: InstanceCreateRequest) -> dict[str, Any]:
    inst = manager.create_instance(
        display_name=req.display_name,
        world_name=req.world_name,
        seed=req.seed,
        difficulty=req.difficulty,
        gamemode=req.gamemode,
        max_players=req.max_players,
    )
    if broadcaster.target is None:
        broadcaster.set_target_instance(inst)
        manager.set_setting("broadcaster_instance_id", inst["id"])
    await _broadcast_telemetry_snapshot()
    return {"instance": inst, "broadcaster": broadcaster.get_status()}


@app.put("/api/instances/{instance_id}")
async def api_update_instance(instance_id: str, req: InstanceUpdateRequest) -> dict[str, Any]:
    try:
        inst = manager.update_instance(
            instance_id=instance_id,
            display_name=req.display_name,
            world_name=req.world_name,
            seed=req.seed,
            difficulty=req.difficulty,
            gamemode=req.gamemode,
            max_players=req.max_players,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    if broadcaster.target and broadcaster.target.id == instance_id:
        broadcaster.set_target_instance(inst)
    await _broadcast_telemetry_snapshot()
    return {"instance": inst}


@app.delete("/api/instances/{instance_id}")
async def api_delete_instance(instance_id: str) -> dict[str, Any]:
    inst = manager.get_instance(instance_id)
    if not inst:
        raise HTTPException(status_code=404, detail="Instance not found")

    await manager.delete_instance(instance_id)
    if broadcaster.target and broadcaster.target.id == instance_id:
        broadcaster.set_target_instance(None)
        manager.set_setting("broadcaster_instance_id", "")
        _sync_broadcaster_target_metadata()

    await _broadcast_telemetry_snapshot()
    return {"deleted": instance_id, "broadcaster": broadcaster.get_status()}


@app.post("/api/instances/{instance_id}/start")
async def api_start_instance(instance_id: str) -> dict[str, Any]:
    try:
        inst = await manager.start_instance(instance_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    # If the currently selected broadcaster target is offline, or matches this instance, sync it
    if broadcaster.target is None or broadcaster.target.id == instance_id:
        broadcaster.set_target_instance(inst)
        manager.set_setting("broadcaster_instance_id", inst["id"])
    else:
        current_target = manager.get_instance(broadcaster.target.id)
        if not current_target or current_target["status"] != "online":
            broadcaster.set_target_instance(inst)
            manager.set_setting("broadcaster_instance_id", inst["id"])

    await _broadcast_telemetry_snapshot()
    return {"instance": inst, "broadcaster": broadcaster.get_status()}


@app.post("/api/instances/{instance_id}/stop")
async def api_stop_instance(instance_id: str) -> dict[str, Any]:
    if not manager.get_instance(instance_id):
        raise HTTPException(status_code=404, detail="Instance not found")
    inst = await manager.stop_instance(instance_id)
    await _broadcast_telemetry_snapshot()
    return {"instance": inst}


@app.post("/api/instances/{instance_id}/restart")
async def api_restart_instance(instance_id: str) -> dict[str, Any]:
    if not manager.get_instance(instance_id):
        raise HTTPException(status_code=404, detail="Instance not found")
    inst = await manager.restart_instance(instance_id)
    if broadcaster.target and broadcaster.target.id == instance_id:
        broadcaster.set_target_instance(inst)
    await _broadcast_telemetry_snapshot()
    return {"instance": inst}


@app.post("/api/instances/{instance_id}/kill")
async def api_kill_instance(instance_id: str) -> dict[str, Any]:
    if not manager.get_instance(instance_id):
        raise HTTPException(status_code=404, detail="Instance not found")
    inst = await manager.kill_instance(instance_id)
    await _broadcast_telemetry_snapshot()
    return {"instance": inst}


@app.post("/api/instances/{instance_id}/command")
async def api_send_command(instance_id: str, req: ConsoleCommandRequest) -> dict[str, Any]:
    if not manager.get_instance(instance_id):
        raise HTTPException(status_code=404, detail="Instance not found")
    try:
        await manager.send_command(instance_id, req.command)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "instance_id": instance_id, "command": req.command}


@app.get("/api/instances/{instance_id}/logs")
async def api_get_logs(instance_id: str) -> dict[str, Any]:
    if not manager.get_instance(instance_id):
        raise HTTPException(status_code=404, detail="Instance not found")
    return {
        "instance_id": instance_id,
        "logs": manager.get_console_logs(instance_id),
    }


@app.get("/api/instances/{instance_id}/properties")
async def api_get_properties(instance_id: str) -> dict[str, Any]:
    inst = manager.get_instance(instance_id)
    if not inst:
        raise HTTPException(status_code=404, detail="Instance not found")
    props_path = manager.instance_dir(instance_id) / "server.properties"
    content = props_path.read_text(encoding="utf-8") if props_path.exists() else ""
    return {"instance_id": instance_id, "content": content}


# -----------------------------------------------------------------------------
# LAN Broadcaster & Console Switch Bridge Endpoints
# -----------------------------------------------------------------------------


@app.get("/api/broadcaster")
async def api_get_broadcaster() -> dict[str, Any]:
    _sync_broadcaster_target_metadata()
    return broadcaster.get_status()


@app.post("/api/broadcaster")
async def api_update_broadcaster(req: BroadcasterUpdateRequest) -> dict[str, Any]:
    if req.enabled is not None:
        broadcaster.set_enabled(req.enabled)
        manager.set_setting("broadcaster_enabled", "true" if req.enabled else "false")

    if req.target_instance_id is not None:
        if req.target_instance_id == "":
            broadcaster.set_target_instance(None)
            manager.set_setting("broadcaster_instance_id", "")
        else:
            inst = manager.get_instance(req.target_instance_id)
            if not inst:
                raise HTTPException(status_code=404, detail="Selected target instance not found")
            broadcaster.set_target_instance(inst)
            manager.set_setting("broadcaster_instance_id", inst["id"])

    await _broadcast_telemetry_snapshot()
    return broadcaster.get_status()


@app.post("/api/broadcaster/test-ping")
async def api_test_broadcaster_ping() -> dict[str, Any]:
    """
    Send an authentic UDP RakNet `ID_UNCONNECTED_PING` (0x01) packet to 127.0.0.1:19132
    and decode the returned `ID_UNCONNECTED_PONG` (0x1c) to verify LAN discovery live.
    """
    if not broadcaster.enabled:
        raise HTTPException(status_code=400, detail="LAN Broadcaster is currently disabled")
    if not broadcaster.target:
        raise HTTPException(status_code=400, detail="No target instance is selected for LAN broadcast")

    def _sync_ping() -> dict[str, Any]:
        t0 = time.perf_counter()
        ping_ts = int(time.time() * 1000)
        pkt = (
            bytes([ID_UNCONNECTED_PING])
            + struct.pack(">Q", ping_ts)
            + RAKNET_MAGIC
            + struct.pack(">Q", 0xCAFEBABE19132)
        )
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(2.0)
            sock.sendto(pkt, ("127.0.0.1", broadcaster.bind_port))
            data, addr = sock.recvfrom(2048)
        rtt_ms = round((time.perf_counter() - t0) * 1000, 2)
        parsed = parse_unconnected_pong(data)
        if not parsed:
            raise RuntimeError("Received invalid RakNet Pong response")
        parsed["rtt_ms"] = rtt_ms
        parsed["responder"] = f"{addr[0]}:{addr[1]}"
        return parsed

    try:
        result = await asyncio.to_thread(_sync_ping)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"RakNet Ping failed: {exc}") from exc

    await _broadcast_telemetry_snapshot()
    return result


# -----------------------------------------------------------------------------
# World & Backup Automation Endpoints
# -----------------------------------------------------------------------------


@app.get("/api/backups")
async def api_list_backups(instance_id: str | None = None) -> dict[str, Any]:
    return {"backups": manager.list_backups(instance_id=instance_id)}


@app.post("/api/instances/{instance_id}/backups")
async def api_create_backup(instance_id: str) -> dict[str, Any]:
    try:
        backup = await manager.create_backup(instance_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Backup failed: {exc}") from exc

    return {"backup": backup, "backups": manager.list_backups()}


@app.post("/api/instances/{instance_id}/backups/{filename}/restore")
async def api_restore_backup(instance_id: str, filename: str) -> dict[str, Any]:
    try:
        inst = await manager.restore_backup(instance_id, filename)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Restore failed: {exc}") from exc

    await _broadcast_telemetry_snapshot()
    return {"instance": inst}


@app.post("/api/instances/{instance_id}/worlds/import")
async def api_import_world(instance_id: str, file: UploadFile = File(...)) -> dict[str, Any]:
    filename = file.filename or "world.mcworld"
    if not filename.lower().endswith((".mcworld", ".zip")):
        raise HTTPException(
            status_code=400,
            detail="Invalid file format. Please upload a .mcworld or .zip archive.",
        )

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    try:
        inst = await manager.import_world_archive(instance_id, filename, content)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"World import failed: {exc}") from exc

    await _broadcast_telemetry_snapshot()
    return {"instance": inst, "backups": manager.list_backups()}


@app.get("/api/backups/{filename}/download")
async def api_download_backup(filename: str) -> FileResponse:
    safe_name = Path(filename).name
    target = BACKUPS_DIR / safe_name
    if not target.exists():
        raise HTTPException(status_code=404, detail="Backup file not found")
    return FileResponse(
        path=str(target),
        filename=safe_name,
        media_type="application/zip",
    )


@app.delete("/api/backups/{filename}")
async def api_delete_backup(filename: str) -> dict[str, Any]:
    try:
        manager.delete_backup(filename)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"deleted": filename, "backups": manager.list_backups()}


# -----------------------------------------------------------------------------
# WebSocket Real-Time Telemetry & Interactive Console Endpoint
# -----------------------------------------------------------------------------


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    await ws_hub.connect(ws)
    try:
        _sync_broadcaster_target_metadata()
        instances = manager.list_instances()
        logs_by_instance = {inst["id"]: manager.get_console_logs(inst["id"]) for inst in instances}
        await ws.send_json(
            {
                "type": "init",
                "instances": instances,
                "broadcaster": broadcaster.get_status(),
                "backups": manager.list_backups(),
                "host": _get_host_metrics(),
                "logs": logs_by_instance,
            }
        )

        while True:
            msg = await ws.receive_json()
            action = msg.get("action")
            if action == "command":
                inst_id = msg.get("instance_id", "")
                cmd = msg.get("command", "")
                if inst_id and cmd:
                    try:
                        await manager.send_command(inst_id, cmd)
                    except Exception as exc:
                        await ws.send_json(
                            {
                                "type": "error",
                                "message": str(exc),
                            }
                        )
            elif action == "subscribe_logs":
                inst_id = msg.get("instance_id", "")
                if inst_id:
                    await ws.send_json(
                        {
                            "type": "console_history",
                            "instance_id": inst_id,
                            "logs": manager.get_console_logs(inst_id),
                        }
                    )
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        logger.debug("WebSocket session ended: %s", exc)
    finally:
        ws_hub.disconnect(ws)

