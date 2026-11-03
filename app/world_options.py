"""
Comprehensive Minecraft Bedrock World & Server Options Engine (`/app/world_options.py`).

Provides:
- Complete schema and normalization for 44+ Minecraft Bedrock Server & World options:
  - Core World & Gameplay: Hardcore Mode, World Type (Infinite / Flat / Legacy), Force Gamemode, Seed, Difficulty, Max Players
  - Starting Gear & HUD: Bonus Starting Chest, Starting Locator Map, Show Player Coordinates, Show Days Played, Player Waypoints/Locator, Recipe Unlocking, Limited Crafting, Death Messages, Recipe Toasts
  - Combat, Survival & Respawn: PvP / Friendly Fire, Keep Inventory, Immediate Respawn, Natural Regeneration, Fall/Fire/Drowning/Freeze Damage, Respawn Radius, Players Sleeping % (1-Player Sleep)
  - World Cycle, Mobs, Drops & Explosions: Daylight Cycle, Weather Cycle, Mob Spawning, Mob Griefing, Mob Loot, Tile Drops, Entity Drops, Insomnia/Phantoms, Fire Spread, TNT Explodes, TNT Drop Decay, Respawn Blocks Explode, Projectiles Break Chorus/Pots, Random Tick Speed
  - Cheats, Permissions & Education: Allow Cheats, Default Player Permission Level, Command Blocks Enabled, Command Block Output, Send Command Feedback, Education Edition Features (Chemistry), Immutable World
  - Server Performance, Security & Anti-Cheat: View Distance, Simulation/Tick Distance, Idle Kick Timeout, Max CPU Threads, Xbox Online Mode, Allowlist, Texture Pack Required, Chat Restriction, Disable Player Interaction, Disable Custom Skins/Persona/Emotes, Client-Side Chunk Generation, Movement Authority, Block Breaking Authority
- Pure-Python Little-Endian Bedrock `level.dat` NBT parser and writer (8-byte header `<II` + Little-Endian NBT Compound).
- Embedded canonical Bedrock 1.26.61.1 `level.dat` template so even brand-new worlds have `bonusChestEnabled`, `startWithMapEnabled`, `IsHardcore`, `Generator`, `showcoordinates`, and all gamerules configured before `bedrock_server.exe` first boots.
- Live STDIN `/gamerule` command builder so option changes can be applied immediately to a running server.
"""

from __future__ import annotations

import base64
import hashlib
import struct
import time
from pathlib import Path
from typing import Any
import zlib

# Canonical compressed Bedrock 1.26.61.1 Little-Endian NBT `level.dat` template (3,060 bytes uncompressed)
_VANILLA_LEVEL_DAT_B64 = (
    "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"
    "9D/3fL/t"
)


DEFAULT_WORLD_OPTIONS: dict[str, Any] = {
    # 1. Core World & Gameplay
    "hardcore": False,
    "world_type": "DEFAULT",  # DEFAULT (Infinite=1), FLAT (Superflat=2), LEGACY (Old=0)
    "force_gamemode": False,

    # 2. Starting Gear, HUD & Player Experience
    "bonus_chest": False,
    "starting_map": False,
    "starter_kit": "none",  # none | wood_stone | iron_coop | homestead_builder | diamond_champion
    "starter_chest_loadout": "none",  # none | coop_gear_chest | house_builder_chest
    "starter_house": "none",  # none | camp_pad | coop_cabin | stone_fortress
    "show_coordinates": True,
    "show_days_played": False,
    "player_locator": True,
    "recipes_unlock": True,
    "limited_crafting": False,
    "show_recipe_messages": True,
    "show_death_messages": True,
    "show_border_effect": True,
    "show_tags": True,

    # 3. Combat, Survival, Death & Respawn Rules
    "pvp": True,
    "keep_inventory": False,
    "immediate_respawn": False,
    "natural_regeneration": True,
    "fall_damage": True,
    "fire_damage": True,
    "drowning_damage": True,
    "freeze_damage": True,
    "spawn_radius": 5,
    "players_sleeping_percentage": 100,

    # 4. World Cycle, Mobs, Drops & Explosions
    "daylight_cycle": True,
    "weather_cycle": True,
    "mob_spawning": True,
    "mob_griefing": True,
    "mob_loot": True,
    "tile_drops": True,
    "entity_drops": True,
    "insomnia": True,
    "fire_spreads": True,
    "tnt_explodes": True,
    "tnt_explosion_drop_decay": True,
    "respawn_blocks_explode": True,
    "projectiles_can_break_blocks": True,
    "random_tick_speed": 1,

    # 5. Cheats, Permissions, Commands & Education
    "allow_cheats": True,
    "default_permission_level": "member",  # visitor | member | operator
    "command_blocks_enabled": True,
    "command_block_output": True,
    "send_command_feedback": True,
    "education_features": False,
    "immutable_world": False,

    # 6. Server Performance, Security, Chat & Anti-Cheat (server.properties)
    "online_mode": False,
    "allow_list": False,
    "view_distance": 32,
    "tick_distance": 4,
    "player_idle_timeout": 30,
    "max_threads": 8,
    "chat_restriction": "None",  # None | Dropped | Disabled
    "disable_player_interaction": False,
    "disable_custom_skins": False,
    "disable_persona": False,
    "disable_emote_chat": False,
    "texturepack_required": False,
    "client_side_chunk_generation": True,
    "server_authoritative_movement": "server-auth",  # client-auth | server-auth | server-auth-with-rewind
    "server_authoritative_block_breaking": False,
}


def _clamp_int(val: Any, default: int, low: int, high: int) -> int:
    try:
        return max(low, min(high, int(val)))
    except (TypeError, ValueError):
        return default


def _to_bool(val: Any, default: bool) -> bool:
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(val)
    if isinstance(val, str):
        s = val.strip().lower()
        if s in ("true", "1", "yes", "on"):
            return True
        if s in ("false", "0", "no", "off"):
            return False
    return default


def normalize_world_options(
    raw: dict[str, Any] | None,
    base: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Merge and sanitize user-supplied world & server options against `DEFAULT_WORLD_OPTIONS`."""
    merged = dict(DEFAULT_WORLD_OPTIONS)
    if base:
        for k in DEFAULT_WORLD_OPTIONS:
            if k in base:
                merged[k] = base[k]
    if raw:
        for k in DEFAULT_WORLD_OPTIONS:
            if k in raw and raw[k] is not None:
                merged[k] = raw[k]

    bool_keys = (
        "hardcore",
        "force_gamemode",
        "bonus_chest",
        "starting_map",
        "show_coordinates",
        "show_days_played",
        "player_locator",
        "recipes_unlock",
        "limited_crafting",
        "show_recipe_messages",
        "show_death_messages",
        "show_border_effect",
        "show_tags",
        "pvp",
        "keep_inventory",
        "immediate_respawn",
        "natural_regeneration",
        "fall_damage",
        "fire_damage",
        "drowning_damage",
        "freeze_damage",
        "daylight_cycle",
        "weather_cycle",
        "mob_spawning",
        "mob_griefing",
        "mob_loot",
        "tile_drops",
        "entity_drops",
        "insomnia",
        "fire_spreads",
        "tnt_explodes",
        "tnt_explosion_drop_decay",
        "respawn_blocks_explode",
        "projectiles_can_break_blocks",
        "allow_cheats",
        "command_blocks_enabled",
        "command_block_output",
        "send_command_feedback",
        "education_features",
        "immutable_world",
        "online_mode",
        "allow_list",
        "disable_player_interaction",
        "disable_custom_skins",
        "disable_persona",
        "disable_emote_chat",
        "texturepack_required",
        "client_side_chunk_generation",
        "server_authoritative_block_breaking",
    )
    for bk in bool_keys:
        merged[bk] = _to_bool(merged.get(bk), bool(DEFAULT_WORLD_OPTIONS[bk]))

    wt = str(merged.get("world_type", "DEFAULT")).upper().strip()
    if wt not in ("DEFAULT", "FLAT", "LEGACY"):
        wt = "DEFAULT"
    merged["world_type"] = wt

    perm = str(merged.get("default_permission_level", "member")).lower().strip()
    if perm not in ("visitor", "member", "operator"):
        perm = "member"
    merged["default_permission_level"] = perm

    chat = str(merged.get("chat_restriction", "None")).strip()
    if chat not in ("None", "Dropped", "Disabled"):
        chat = "None"
    merged["chat_restriction"] = chat

    auth_move = str(merged.get("server_authoritative_movement", "server-auth")).lower().strip()
    if auth_move not in ("client-auth", "server-auth", "server-auth-with-rewind"):
        auth_move = "server-auth"
    merged["server_authoritative_movement"] = auth_move

    sk = str(merged.get("starter_kit", "none")).lower().strip()
    if sk not in ("none", "wood_stone", "iron_coop", "homestead_builder", "diamond_champion"):
        sk = "none"
    merged["starter_kit"] = sk

    sc = str(merged.get("starter_chest_loadout", "none")).lower().strip()
    if sc not in ("none", "coop_gear_chest", "house_builder_chest"):
        sc = "none"
    merged["starter_chest_loadout"] = sc

    sh = str(merged.get("starter_house", "none")).lower().strip()
    if sh not in ("none", "camp_pad", "coop_cabin", "stone_fortress"):
        sh = "none"
    merged["starter_house"] = sh

    merged["spawn_radius"] = _clamp_int(merged.get("spawn_radius"), 5, 0, 128)
    merged["players_sleeping_percentage"] = _clamp_int(
        merged.get("players_sleeping_percentage"), 100, 0, 100
    )
    merged["random_tick_speed"] = _clamp_int(merged.get("random_tick_speed"), 1, 0, 4096)
    merged["view_distance"] = _clamp_int(merged.get("view_distance"), 32, 4, 96)
    merged["tick_distance"] = _clamp_int(merged.get("tick_distance"), 4, 4, 12)
    merged["player_idle_timeout"] = _clamp_int(merged.get("player_idle_timeout"), 30, 0, 1440)
    merged["max_threads"] = _clamp_int(merged.get("max_threads"), 8, 0, 64)

    return merged


def parse_seed_to_int64(seed_str: str) -> int:
    """
    Convert a user seed string to a signed 64-bit integer using Minecraft's seed conversion rules
    (direct integer if numeric, otherwise Java/Bedrock string hashCode / SHA-derived 64-bit int).
    """
    s = (seed_str or "").strip()
    if not s:
        # Generate a non-zero 64-bit seed
        return struct.unpack("<q", hashlib.sha256(str(time.time_ns()).encode()).digest()[:8])[0]
    try:
        val = int(s)
        # Wrap into signed 64-bit range
        return ((val + (1 << 63)) % (1 << 64)) - (1 << 63)
    except ValueError:
        # Classic 32-bit signed hashCode used by Minecraft for text seeds
        h = 0
        for ch in s:
            h = (31 * h + ord(ch)) & 0xFFFFFFFF
        if h & 0x80000000:
            h -= 0x100000000
        return int(h)


# -----------------------------------------------------------------------------
# Pure-Python Little-Endian Bedrock `level.dat` NBT Parser & Serializer
# -----------------------------------------------------------------------------


def parse_bedrock_level_dat(raw_bytes: bytes) -> tuple[int, str, dict[str, tuple[int, Any]]]:
    """
    Parse a Bedrock `level.dat` file (8-byte header `<II` version + length, followed by Little-Endian NBT).
    Returns `(header_version, root_name, compound_dict)`.
    """
    if len(raw_bytes) < 12:
        raise ValueError("level.dat too short")
    ver, _length = struct.unpack_from("<II", raw_bytes, 0)
    data = raw_bytes[8:]
    pos = 0

    def read_str() -> str:
        nonlocal pos
        slen = struct.unpack_from("<H", data, pos)[0]
        pos += 2
        b = data[pos : pos + slen]
        pos += slen
        return b.decode("utf-8", "surrogateescape")

    def read_val(tag_type: int) -> tuple[int, Any]:
        nonlocal pos
        if tag_type == 1:
            v = struct.unpack_from("<b", data, pos)[0]
            pos += 1
            return (1, v)
        if tag_type == 2:
            v = struct.unpack_from("<h", data, pos)[0]
            pos += 2
            return (2, v)
        if tag_type == 3:
            v = struct.unpack_from("<i", data, pos)[0]
            pos += 4
            return (3, v)
        if tag_type == 4:
            v = struct.unpack_from("<q", data, pos)[0]
            pos += 8
            return (4, v)
        if tag_type == 5:
            v = struct.unpack_from("<f", data, pos)[0]
            pos += 4
            return (5, v)
        if tag_type == 6:
            v = struct.unpack_from("<d", data, pos)[0]
            pos += 8
            return (6, v)
        if tag_type == 7:
            alen = struct.unpack_from("<i", data, pos)[0]
            pos += 4
            b = data[pos : pos + alen]
            pos += alen
            return (7, b)
        if tag_type == 8:
            return (8, read_str())
        if tag_type == 9:
            elem_type = data[pos]
            pos += 1
            llen = struct.unpack_from("<i", data, pos)[0]
            pos += 4
            items = [read_val(elem_type)[1] for _ in range(llen)]
            return (9, (elem_type, items))
        if tag_type == 10:
            comp: dict[str, tuple[int, Any]] = {}
            while pos < len(data):
                sub_type = data[pos]
                pos += 1
                if sub_type == 0:
                    break
                key = read_str()
                comp[key] = read_val(sub_type)
            return (10, comp)
        if tag_type == 11:
            alen = struct.unpack_from("<i", data, pos)[0]
            pos += 4
            b = data[pos : pos + 4 * alen]
            pos += 4 * alen
            return (11, (alen, b))
        if tag_type == 12:
            alen = struct.unpack_from("<i", data, pos)[0]
            pos += 4
            b = data[pos : pos + 8 * alen]
            pos += 8 * alen
            return (12, (alen, b))
        raise ValueError(f"Unsupported NBT tag type: {tag_type}")

    root_type = data[pos]
    pos += 1
    if root_type != 10:
        raise ValueError(f"Expected root TAG_Compound (10), got {root_type}")
    root_name = read_str()
    _, root_compound = read_val(10)
    return ver, root_name, root_compound


def serialize_bedrock_level_dat(
    ver: int,
    root_name: str,
    root_compound: dict[str, tuple[int, Any]],
) -> bytes:
    """Serialize `(ver, root_name, root_compound)` back into Bedrock Little-Endian `level.dat` bytes."""
    out = bytearray()

    def write_str(s: str) -> None:
        b = s.encode("utf-8", "surrogateescape")
        out.extend(struct.pack("<H", len(b)))
        out.extend(b)

    def write_val(tag_type: int, val: Any) -> None:
        if tag_type == 1:
            out.extend(struct.pack("<b", int(val)))
        elif tag_type == 2:
            out.extend(struct.pack("<h", int(val)))
        elif tag_type == 3:
            out.extend(struct.pack("<i", int(val)))
        elif tag_type == 4:
            out.extend(struct.pack("<q", int(val)))
        elif tag_type == 5:
            out.extend(struct.pack("<f", float(val)))
        elif tag_type == 6:
            out.extend(struct.pack("<d", float(val)))
        elif tag_type == 7:
            out.extend(struct.pack("<i", len(val)))
            out.extend(val)
        elif tag_type == 8:
            write_str(str(val))
        elif tag_type == 9:
            elem_type, items = val
            out.append(int(elem_type))
            out.extend(struct.pack("<i", len(items)))
            for it in items:
                write_val(elem_type, it)
        elif tag_type == 10:
            for k, (sub_t, sub_v) in val.items():
                out.append(int(sub_t))
                write_str(k)
                write_val(sub_t, sub_v)
            out.append(0)
        elif tag_type in (11, 12):
            alen, raw_b = val
            out.extend(struct.pack("<i", int(alen)))
            out.extend(raw_b)
        else:
            raise ValueError(f"Unsupported NBT tag type: {tag_type}")

    out.append(10)
    write_str(root_name)
    write_val(10, root_compound)
    return struct.pack("<II", int(ver), len(out)) + bytes(out)


def get_vanilla_level_dat_bytes() -> bytes:
    """Decompress and return the canonical Bedrock 1.26.61.1 `level.dat` binary template."""
    return zlib.decompress(base64.b64decode(_VANILLA_LEVEL_DAT_B64))


def read_options_from_level_dat(
    world_dir: Path,
    base_options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Read existing world options from `worlds/<world_name>/level.dat` if present."""
    opts = normalize_world_options(base_options)
    level_dat_path = world_dir / "level.dat"
    if not level_dat_path.exists():
        return opts
    try:
        raw = level_dat_path.read_bytes()
        if len(raw) < 512 or raw.lstrip().startswith(b"{"):
            return opts
        _, _, comp = parse_bedrock_level_dat(raw)

        def get_b(key: str, default: bool) -> bool:
            if key in comp:
                return bool(comp[key][1])
            return default

        def get_i(key: str, default: int) -> int:
            if key in comp:
                return int(comp[key][1])
            return default

        gen_val = get_i("Generator", 1)
        gen_rev = {0: "LEGACY", 1: "DEFAULT", 2: "FLAT"}
        opts["world_type"] = gen_rev.get(gen_val, opts["world_type"])
        opts["hardcore"] = get_b("IsHardcore", opts["hardcore"])
        opts["force_gamemode"] = get_b("ForceGameType", opts["force_gamemode"])
        opts["bonus_chest"] = get_b("bonusChestEnabled", opts["bonus_chest"])
        opts["starting_map"] = get_b("startWithMapEnabled", opts["starting_map"])
        opts["show_coordinates"] = get_b("showcoordinates", opts["show_coordinates"])
        opts["show_days_played"] = get_b("showdaysplayed", opts["show_days_played"])
        opts["player_locator"] = bool(get_i("playerwaypoints", 1 if opts["player_locator"] else 0))
        opts["recipes_unlock"] = get_b("recipesunlock", opts["recipes_unlock"])
        opts["limited_crafting"] = get_b("dolimitedcrafting", opts["limited_crafting"])
        opts["show_recipe_messages"] = get_b("showrecipemessages", opts["show_recipe_messages"])
        opts["show_death_messages"] = get_b("showdeathmessages", opts["show_death_messages"])
        opts["show_border_effect"] = get_b("showbordereffect", opts["show_border_effect"])
        opts["show_tags"] = get_b("showtags", opts["show_tags"])
        opts["pvp"] = get_b("pvp", opts["pvp"])
        opts["keep_inventory"] = get_b("keepinventory", opts["keep_inventory"])
        opts["immediate_respawn"] = get_b("doimmediaterespawn", opts["immediate_respawn"])
        opts["natural_regeneration"] = get_b("naturalregeneration", opts["natural_regeneration"])
        opts["fall_damage"] = get_b("falldamage", opts["fall_damage"])
        opts["fire_damage"] = get_b("firedamage", opts["fire_damage"])
        opts["drowning_damage"] = get_b("drowningdamage", opts["drowning_damage"])
        opts["freeze_damage"] = get_b("freezedamage", opts["freeze_damage"])
        opts["spawn_radius"] = get_i("spawnradius", opts["spawn_radius"])
        opts["players_sleeping_percentage"] = get_i(
            "playerssleepingpercentage", opts["players_sleeping_percentage"]
        )
        opts["daylight_cycle"] = get_b("dodaylightcycle", opts["daylight_cycle"])
        opts["weather_cycle"] = get_b("doweathercycle", opts["weather_cycle"])
        opts["mob_spawning"] = get_b("domobspawning", opts["mob_spawning"])
        opts["mob_griefing"] = get_b("mobgriefing", opts["mob_griefing"])
        opts["mob_loot"] = get_b("domobloot", opts["mob_loot"])
        opts["tile_drops"] = get_b("dotiledrops", opts["tile_drops"])
        opts["entity_drops"] = get_b("doentitydrops", opts["entity_drops"])
        opts["insomnia"] = get_b("doinsomnia", opts["insomnia"])
        opts["fire_spreads"] = get_b("dofiretick", opts["fire_spreads"])
        opts["tnt_explodes"] = get_b("tntexplodes", opts["tnt_explodes"])
        opts["tnt_explosion_drop_decay"] = get_b(
            "tntexplosiondropdecay", opts["tnt_explosion_drop_decay"]
        )
        opts["respawn_blocks_explode"] = get_b(
            "respawnblocksexplode", opts["respawn_blocks_explode"]
        )
        opts["projectiles_can_break_blocks"] = get_b(
            "projectilescanbreakblocks", opts["projectiles_can_break_blocks"]
        )
        opts["random_tick_speed"] = get_i("randomtickspeed", opts["random_tick_speed"])
        opts["command_blocks_enabled"] = get_b(
            "commandblocksenabled", opts["command_blocks_enabled"]
        )
        opts["command_block_output"] = get_b("commandblockoutput", opts["command_block_output"])
        opts["send_command_feedback"] = get_b(
            "sendcommandfeedback", opts["send_command_feedback"]
        )
        opts["education_features"] = get_b(
            "educationFeaturesEnabled", opts["education_features"]
        )
        opts["immutable_world"] = get_b("immutableWorld", opts["immutable_world"])
    except Exception:
        pass
    return normalize_world_options(opts)


def apply_options_to_level_dat(
    world_dir: Path,
    instance: dict[str, Any],
    options: dict[str, Any] | None = None,
) -> Path:
    """
    Create or update `worlds/<world_name>/level.dat` with all configured world & gameplay options.
    - If `level.dat` does not exist yet (or is a tiny/JSON stub), initializes a fresh binary NBT
      `level.dat` from the canonical 1.26 template with `bonusChestSpawned=0` and `SpawnY=32767`
      so `bedrock_server.exe` spawns the Bonus Chest and gives Starting Maps on world creation.
    - If `level.dat` already exists as a valid Bedrock NBT file, updates all option tags in-place
      while preserving world progress (`Time`, `currentTick`, `SpawnX/Y/Z`, `bonusChestSpawned`, etc.).
    """
    opts = normalize_world_options(options, instance.get("options"))
    world_dir.mkdir(parents=True, exist_ok=True)
    level_dat_path = world_dir / "level.dat"
    db_dir = world_dir / "db"
    is_new_world = not db_dir.exists() or not any(db_dir.iterdir())

    raw_bytes: bytes | None = None
    if level_dat_path.exists():
        try:
            existing = level_dat_path.read_bytes()
            if len(existing) >= 512 and not existing.lstrip().startswith(b"{"):
                # Validate it parses cleanly
                parse_bedrock_level_dat(existing)
                raw_bytes = existing
        except Exception:
            raw_bytes = None

    if raw_bytes is None:
        raw_bytes = get_vanilla_level_dat_bytes()
        is_new_world = True

    ver, root_name, comp = parse_bedrock_level_dat(raw_bytes)

    # Map gamemode, difficulty, generator, and permission levels
    gm_map = {"survival": 0, "creative": 1, "adventure": 2, "spectator": 6}
    diff_map = {"peaceful": 0, "easy": 1, "normal": 2, "hard": 3}
    gen_map = {"LEGACY": 0, "DEFAULT": 1, "FLAT": 2}
    perm_map = {"visitor": 0, "member": 1, "operator": 2}

    gamemode_str = str(instance.get("gamemode", "survival")).lower()
    difficulty_str = "hard" if opts["hardcore"] else str(instance.get("difficulty", "normal")).lower()

    comp["LevelName"] = (8, str(instance.get("world_name") or instance.get("display_name") or "BedrockLevel"))
    comp["GameType"] = (3, gm_map.get(gamemode_str, 0))
    comp["ForceGameType"] = (1, 1 if opts["force_gamemode"] else 0)
    comp["Difficulty"] = (3, diff_map.get(difficulty_str, 2))
    comp["IsHardcore"] = (1, 1 if opts["hardcore"] else 0)

    # Seed & World Generator Type (apply seed & generator on new worlds, or generator if explicitly changed)
    if is_new_world:
        seed_val = parse_seed_to_int64(str(instance.get("seed", "")))
        comp["RandomSeed"] = (4, seed_val)
        comp["Generator"] = (3, gen_map.get(opts["world_type"], 1))
        comp["SpawnY"] = (3, 32767)
        comp["Time"] = (4, 0)
        comp["currentTick"] = (4, 0)
        comp["PlayerHasDied"] = (1, 0)
        comp["bonusChestSpawned"] = (1, 0)
    else:
        # Even on existing worlds, if Generator was accidentally 0 (Legacy) from an old stub and user wants DEFAULT/FLAT
        if comp.get("Generator", (3, 1))[1] == 0 and opts["world_type"] != "LEGACY":
            comp["Generator"] = (3, gen_map.get(opts["world_type"], 1))

    # Starting Gear & HUD Options
    comp["bonusChestEnabled"] = (1, 1 if opts["bonus_chest"] else 0)
    comp["startWithMapEnabled"] = (1, 1 if opts["starting_map"] else 0)
    comp["showcoordinates"] = (1, 1 if opts["show_coordinates"] else 0)
    comp["showdaysplayed"] = (1, 1 if opts["show_days_played"] else 0)
    comp["playerwaypoints"] = (3, 1 if opts["player_locator"] else 0)
    comp["recipesunlock"] = (1, 1 if opts["recipes_unlock"] else 0)
    comp["dolimitedcrafting"] = (1, 1 if opts["limited_crafting"] else 0)
    comp["showrecipemessages"] = (1, 1 if opts["show_recipe_messages"] else 0)
    comp["showdeathmessages"] = (1, 1 if opts["show_death_messages"] else 0)
    comp["showbordereffect"] = (1, 1 if opts["show_border_effect"] else 0)
    comp["showtags"] = (1, 1 if opts["show_tags"] else 0)

    # Combat, Survival, Death & Respawn Rules
    comp["pvp"] = (1, 1 if opts["pvp"] else 0)
    comp["keepinventory"] = (1, 1 if opts["keep_inventory"] else 0)
    comp["doimmediaterespawn"] = (1, 1 if opts["immediate_respawn"] else 0)
    comp["naturalregeneration"] = (1, 1 if opts["natural_regeneration"] else 0)
    comp["falldamage"] = (1, 1 if opts["fall_damage"] else 0)
    comp["firedamage"] = (1, 1 if opts["fire_damage"] else 0)
    comp["drowningdamage"] = (1, 1 if opts["drowning_damage"] else 0)
    comp["freezedamage"] = (1, 1 if opts["freeze_damage"] else 0)
    comp["spawnradius"] = (3, int(opts["spawn_radius"]))
    comp["playerssleepingpercentage"] = (3, int(opts["players_sleeping_percentage"]))

    # World Cycle, Mobs, Drops & Explosions
    comp["dodaylightcycle"] = (1, 1 if opts["daylight_cycle"] else 0)
    comp["doweathercycle"] = (1, 1 if opts["weather_cycle"] else 0)
    comp["domobspawning"] = (1, 1 if opts["mob_spawning"] else 0)
    comp["spawnMobs"] = (1, 1 if opts["mob_spawning"] else 0)
    comp["mobgriefing"] = (1, 1 if opts["mob_griefing"] else 0)
    comp["domobloot"] = (1, 1 if opts["mob_loot"] else 0)
    comp["dotiledrops"] = (1, 1 if opts["tile_drops"] else 0)
    comp["doentitydrops"] = (1, 1 if opts["entity_drops"] else 0)
    comp["doinsomnia"] = (1, 1 if opts["insomnia"] else 0)
    comp["dofiretick"] = (1, 1 if opts["fire_spreads"] else 0)
    comp["tntexplodes"] = (1, 1 if opts["tnt_explodes"] else 0)
    comp["tntexplosiondropdecay"] = (1, 1 if opts["tnt_explosion_drop_decay"] else 0)
    comp["respawnblocksexplode"] = (1, 1 if opts["respawn_blocks_explode"] else 0)
    comp["projectilescanbreakblocks"] = (1, 1 if opts["projectiles_can_break_blocks"] else 0)
    comp["randomtickspeed"] = (3, int(opts["random_tick_speed"]))

    # Cheats, Permissions, Commands & Education
    comp["cheatsEnabled"] = (1, 1 if opts["allow_cheats"] else 0)
    comp["commandsEnabled"] = (1, 1 if opts["allow_cheats"] else 0)
    comp["commandblocksenabled"] = (1, 1 if opts["command_blocks_enabled"] else 0)
    comp["commandblockoutput"] = (1, 1 if opts["command_block_output"] else 0)
    comp["sendcommandfeedback"] = (1, 1 if opts["send_command_feedback"] else 0)
    comp["educationFeaturesEnabled"] = (1, 1 if opts["education_features"] else 0)
    comp["immutableWorld"] = (1, 1 if opts["immutable_world"] else 0)
    comp["playerPermissionsLevel"] = (3, perm_map.get(opts["default_permission_level"], 1))

    # Performance & Packs
    comp["serverChunkTickRange"] = (3, int(opts["tick_distance"]))
    comp["texturePacksRequired"] = (1, 1 if opts["texturepack_required"] else 0)
    comp["LANBroadcast"] = (1, 1)
    comp["LANBroadcastIntent"] = (1, 1)
    comp["MultiplayerGame"] = (1, 1)
    comp["MultiplayerGameIntent"] = (1, 1)

    updated_bytes = serialize_bedrock_level_dat(ver, root_name, comp)
    level_dat_path.write_bytes(updated_bytes)
    (world_dir / "levelname.txt").write_text(str(instance.get("world_name", "BedrockLevel")), encoding="utf-8")
    return level_dat_path


def build_server_properties(instance: dict[str, Any], options: dict[str, Any] | None = None) -> str:
    """Build the complete `server.properties` file content for a Bedrock Dedicated Server instance."""
    opts = normalize_world_options(options, instance.get("options"))
    b = lambda val: "true" if val else "false"
    difficulty = "hard" if opts["hardcore"] else instance["difficulty"]

    lines = [
        "# Minecraft Bedrock Dedicated Server Properties",
        "# Managed automatically by BedrockNexus Multi-Server Manager",
        f"server-name={instance['display_name']}",
        f"gamemode={instance['gamemode']}",
        f"force-gamemode={b(opts['force_gamemode'])}",
        f"difficulty={difficulty}",
        f"is-hardcore={b(opts['hardcore'])}",
        f"allow-cheats={b(opts['allow_cheats'])}",
        f"max-players={instance['max_players']}",
        f"online-mode={b(opts['online_mode'])}",
        f"allow-list={b(opts['allow_list'])}",
        f"server-port={instance['port']}",
        f"server-portv6={instance['ipv6_port']}",
        "enable-lan-visibility=true",
        "transport=nethernet",
        f"view-distance={opts['view_distance']}",
        f"tick-distance={opts['tick_distance']}",
        f"player-idle-timeout={opts['player_idle_timeout']}",
        f"max-threads={opts['max_threads']}",
        f"level-name={instance['world_name']}",
        f"level-seed={instance.get('seed', '')}",
        f"level-type={opts['world_type']}",
        f"default-player-permission-level={opts['default_permission_level']}",
        f"texturepack-required={b(opts['texturepack_required'])}",
        f"chat-restriction={opts['chat_restriction']}",
        f"disable-player-interaction={b(opts['disable_player_interaction'])}",
        f"disable-custom-skins={b(opts['disable_custom_skins'])}",
        f"disable-persona={b(opts['disable_persona'])}",
        f"disable-emote-chat={b(opts['disable_emote_chat'])}",
        f"client-side-chunk-generation-enabled={b(opts['client_side_chunk_generation'])}",
        "content-log-file-enabled=false",
        "compression-threshold=1",
        "compression-algorithm=zlib",
        f"server-authoritative-movement={opts['server_authoritative_movement']}",
        "player-movement-score-threshold=20",
        "player-movement-action-direction-threshold=0.85",
        "player-movement-distance-threshold=0.3",
        "player-movement-duration-threshold-in-ms=500",
        "correct-player-movement=false",
        f"server-authoritative-block-breaking={b(opts['server_authoritative_block_breaking'])}",
    ]
    return "\n".join(lines) + "\n"


def build_live_gamerule_commands(
    instance: dict[str, Any],
    options: dict[str, Any] | None = None,
) -> list[str]:
    """
    Build a list of console `gamerule` and `difficulty` commands to dispatch to a running
    `bedrock_server.exe` process so option changes take effect immediately in real time.
    """
    opts = normalize_world_options(options, instance.get("options"))
    b = lambda val: "true" if val else "false"
    difficulty = "hard" if opts["hardcore"] else instance.get("difficulty", "normal")

    cmds = [
        f"difficulty {difficulty}",
        f"gamerule showcoordinates {b(opts['show_coordinates'])}",
        f"gamerule showdaysplayed {b(opts['show_days_played'])}",
        f"gamerule recipesunlock {b(opts['recipes_unlock'])}",
        f"gamerule dolimitedcrafting {b(opts['limited_crafting'])}",
        f"gamerule showrecipemessages {b(opts['show_recipe_messages'])}",
        f"gamerule showdeathmessages {b(opts['show_death_messages'])}",
        f"gamerule showbordereffect {b(opts['show_border_effect'])}",
        f"gamerule showtags {b(opts['show_tags'])}",
        f"gamerule pvp {b(opts['pvp'])}",
        f"gamerule keepinventory {b(opts['keep_inventory'])}",
        f"gamerule doimmediaterespawn {b(opts['immediate_respawn'])}",
        f"gamerule naturalregeneration {b(opts['natural_regeneration'])}",
        f"gamerule falldamage {b(opts['fall_damage'])}",
        f"gamerule firedamage {b(opts['fire_damage'])}",
        f"gamerule drowningdamage {b(opts['drowning_damage'])}",
        f"gamerule freezedamage {b(opts['freeze_damage'])}",
        f"gamerule spawnradius {int(opts['spawn_radius'])}",
        f"gamerule playerssleepingpercentage {int(opts['players_sleeping_percentage'])}",
        f"gamerule dodaylightcycle {b(opts['daylight_cycle'])}",
        f"gamerule doweathercycle {b(opts['weather_cycle'])}",
        f"gamerule domobspawning {b(opts['mob_spawning'])}",
        f"gamerule mobgriefing {b(opts['mob_griefing'])}",
        f"gamerule domobloot {b(opts['mob_loot'])}",
        f"gamerule dotiledrops {b(opts['tile_drops'])}",
        f"gamerule doentitydrops {b(opts['entity_drops'])}",
        f"gamerule doinsomnia {b(opts['insomnia'])}",
        f"gamerule dofiretick {b(opts['fire_spreads'])}",
        f"gamerule tntexplodes {b(opts['tnt_explodes'])}",
        f"gamerule tntexplosiondropdecay {b(opts['tnt_explosion_drop_decay'])}",
        f"gamerule respawnblocksexplode {b(opts['respawn_blocks_explode'])}",
        f"gamerule projectilescanbreakblocks {b(opts['projectiles_can_break_blocks'])}",
        f"gamerule randomtickspeed {int(opts['random_tick_speed'])}",
        f"gamerule commandblocksenabled {b(opts['command_blocks_enabled'])}",
        f"gamerule commandblockoutput {b(opts['command_block_output'])}",
        f"gamerule sendcommandfeedback {b(opts['send_command_feedback'])}",
    ]
    return cmds


# -----------------------------------------------------------------------------
# Starter Inventory Kits, Stocked Supply Chests & Auto-Built Co-Op Houses
# -----------------------------------------------------------------------------


def build_starter_kit_commands(
    kit_tier: str,
    selector: str = "@a[tag=!bn_starter_given]",
    tag_after: bool = True,
) -> list[str]:
    """
    Build console commands to equip and give a starter inventory kit to `selector`.
    When `tag_after=True`, tags recipients with `bn_starter_given` so each player receives
    their starter kit automatically once upon joining the world.
    """
    tier = (kit_tier or "none").lower().strip()
    if tier == "none":
        return []

    cmds: list[str] = ["changesetting allow-cheats true"]

    if tier == "wood_stone":
        cmds.extend(
            [
                f"give {selector} stone_sword 1",
                f"give {selector} stone_pickaxe 1",
                f"give {selector} stone_axe 1",
                f"give {selector} stone_shovel 1",
                f"give {selector} stone_hoe 1",
                f"give {selector} shield 1",
                f"give {selector} bed 1 14",
                f"give {selector} torch 64",
                f"give {selector} cooked_beef 32",
                f"give {selector} crafting_table 1",
                f"give {selector} furnace 1",
                f"give {selector} chest 2",
                f"give {selector} oak_planks 64",
                f"give {selector} cobblestone 64",
                f"replaceitem entity {selector} slot.armor.head 0 leather_helmet 1",
                f"replaceitem entity {selector} slot.armor.chest 0 leather_chestplate 1",
                f"replaceitem entity {selector} slot.armor.legs 0 leather_leggings 1",
                f"replaceitem entity {selector} slot.armor.feet 0 leather_boots 1",
            ]
        )
    elif tier == "iron_coop":
        cmds.extend(
            [
                f"give {selector} iron_sword 1",
                f"give {selector} iron_pickaxe 1",
                f"give {selector} iron_axe 1",
                f"give {selector} iron_shovel 1",
                f"give {selector} iron_hoe 1",
                f"give {selector} shield 1",
                f"give {selector} bow 1",
                f"give {selector} arrow 64",
                f"give {selector} bed 1 14",
                f"give {selector} torch 64",
                f"give {selector} cooked_beef 64",
                f"give {selector} golden_apple 8",
                f"give {selector} water_bucket 1",
                f"give {selector} empty_map 1",
                f"give {selector} compass 1",
                f"give {selector} crafting_table 1",
                f"give {selector} furnace 1",
                f"give {selector} chest 2",
                f"replaceitem entity {selector} slot.armor.head 0 iron_helmet 1",
                f"replaceitem entity {selector} slot.armor.chest 0 iron_chestplate 1",
                f"replaceitem entity {selector} slot.armor.legs 0 iron_leggings 1",
                f"replaceitem entity {selector} slot.armor.feet 0 iron_boots 1",
            ]
        )
    elif tier == "homestead_builder":
        cmds.extend(
            [
                # Full Iron Gear & Armor
                f"give {selector} iron_sword 1",
                f"give {selector} iron_pickaxe 1",
                f"give {selector} iron_axe 1",
                f"give {selector} iron_shovel 1",
                f"give {selector} shield 1",
                f"give {selector} bow 1",
                f"give {selector} arrow 64",
                f"give {selector} bed 2 14",
                f"give {selector} torch 64",
                f"give {selector} lantern 16",
                f"give {selector} cooked_beef 64",
                f"give {selector} golden_apple 8",
                f"give {selector} water_bucket 1",
                f"give {selector} empty_map 1",
                # Complete House Building Materials & Workstations in Inventory
                f"give {selector} oak_planks 256",
                f"give {selector} cobblestone 256",
                f"give {selector} oak_log 64",
                f"give {selector} stone_bricks 128",
                f"give {selector} glass 64",
                f"give {selector} oak_stairs 64",
                f"give {selector} wooden_door 4",
                f"give {selector} oak_fence 32",
                f"give {selector} crafting_table 2",
                f"give {selector} furnace 2",
                f"give {selector} blast_furnace 1",
                f"give {selector} smoker 1",
                f"give {selector} anvil 1",
                f"give {selector} chest 4",
                f"replaceitem entity {selector} slot.armor.head 0 iron_helmet 1",
                f"replaceitem entity {selector} slot.armor.chest 0 iron_chestplate 1",
                f"replaceitem entity {selector} slot.armor.legs 0 iron_leggings 1",
                f"replaceitem entity {selector} slot.armor.feet 0 iron_boots 1",
            ]
        )
    elif tier == "diamond_champion":
        cmds.extend(
            [
                f"give {selector} diamond_sword 1",
                f"give {selector} diamond_pickaxe 1",
                f"give {selector} diamond_axe 1",
                f"give {selector} diamond_shovel 1",
                f"give {selector} diamond_hoe 1",
                f"give {selector} shield 1",
                f"give {selector} bow 1",
                f"give {selector} arrow 64",
                f"give {selector} totem_of_undying 1",
                f"give {selector} golden_apple 16",
                f"give {selector} cooked_beef 64",
                f"give {selector} bed 1 14",
                f"give {selector} ender_chest 1",
                f"give {selector} torch 64",
                f"give {selector} lantern 16",
                f"give {selector} water_bucket 1",
                f"give {selector} oak_planks 128",
                f"give {selector} cobblestone 128",
                f"replaceitem entity {selector} slot.armor.head 0 diamond_helmet 1",
                f"replaceitem entity {selector} slot.armor.chest 0 diamond_chestplate 1",
                f"replaceitem entity {selector} slot.armor.legs 0 diamond_leggings 1",
                f"replaceitem entity {selector} slot.armor.feet 0 diamond_boots 1",
            ]
        )

    if tag_after and selector == "@a[tag=!bn_starter_given]":
        cmds.append(f"tag {selector} add bn_starter_given")

    return cmds


def _stock_chest_commands(rel_pos: str, items: list[tuple[int, str, int]], at_selector: str) -> list[str]:
    """Generate `execute at <selector> run replaceitem block <rel_pos> slot.container <slot> <item> <count>`."""
    return [
        f"execute at {at_selector} run replaceitem block {rel_pos} slot.container {slot} {item} {count}"
        for slot, item, count in items
    ]


def build_starter_chest_commands(
    loadout: str,
    at_selector: str = "@a[c=1]",
) -> list[str]:
    """
    Build console commands that place and stock 1 or 2 Co-Op Supply Chests & Workstations
    2 blocks east of the target player (`~2 ~0 ~0` and `~2 ~0 ~1`).
    """
    mode = (loadout or "none").lower().strip()
    if mode == "none":
        return []

    cmds: list[str] = [
        "changesetting allow-cheats true",
        f"execute at {at_selector} run setblock ~2 ~0 ~0 chest",
        f"execute at {at_selector} run setblock ~2 ~1 ~0 torch",
    ]

    # Chest 1: Co-Op Weapons, Tools, Beds, Armor, Food & Navigation (27 slots: 0..26)
    gear_items: list[tuple[int, str, int]] = [
        (0, "bed", 1),
        (1, "bed", 1),
        (2, "bed", 1),
        (3, "bed", 1),
        (4, "iron_sword", 1),
        (5, "iron_sword", 1),
        (6, "iron_pickaxe", 1),
        (7, "iron_pickaxe", 1),
        (8, "iron_axe", 1),
        (9, "iron_axe", 1),
        (10, "iron_shovel", 1),
        (11, "shield", 1),
        (12, "shield", 1),
        (13, "bow", 1),
        (14, "arrow", 64),
        (15, "cooked_beef", 64),
        (16, "bread", 64),
        (17, "golden_apple", 16),
        (18, "torch", 64),
        (19, "lantern", 16),
        (20, "water_bucket", 1),
        (21, "empty_map", 4),
        (22, "compass", 4),
        (23, "crafting_table", 2),
        (24, "furnace", 2),
        (25, "oak_planks", 64),
        (26, "cobblestone", 64),
    ]
    cmds.extend(_stock_chest_commands("~2 ~0 ~0", gear_items, at_selector))

    if mode == "house_builder_chest":
        # Chest 2: Mega House-Builder Materials & Workstations (`~2 ~0 ~2`)
        cmds.extend(
            [
                f"execute at {at_selector} run setblock ~2 ~0 ~2 barrel",
                f"execute at {at_selector} run setblock ~2 ~1 ~2 lantern",
            ]
        )
        builder_items: list[tuple[int, str, int]] = [
            (0, "oak_planks", 64),
            (1, "oak_planks", 64),
            (2, "oak_planks", 64),
            (3, "oak_planks", 64),
            (4, "oak_planks", 64),
            (5, "oak_planks", 64),
            (6, "cobblestone", 64),
            (7, "cobblestone", 64),
            (8, "cobblestone", 64),
            (9, "cobblestone", 64),
            (10, "cobblestone", 64),
            (11, "cobblestone", 64),
            (12, "oak_log", 64),
            (13, "oak_log", 64),
            (14, "stone_bricks", 64),
            (15, "stone_bricks", 64),
            (16, "glass", 64),
            (17, "glass", 64),
            (18, "oak_stairs", 64),
            (19, "oak_fence", 64),
            (20, "wooden_door", 4),
            (21, "lantern", 32),
            (22, "blast_furnace", 2),
            (23, "smoker", 2),
            (24, "anvil", 1),
            (25, "enchanting_table", 1),
            (26, "bookshelf", 15),
        ]
        cmds.extend(_stock_chest_commands("~2 ~0 ~2", builder_items, at_selector))

    return cmds


def build_starter_house_commands(
    house_type: str,
    at_selector: str = "@a[c=1]",
) -> list[str]:
    """
    Build console commands that construct a furnished Co-Op structure right next to `at_selector`
    (offset `~4` blocks on +X so it never buries or traps the player):
    - `camp_pad`: 5x5 Lit Cobblestone & Oak Base Camp with 4 Beds, Workstations & Stocked Chest
    - `coop_cabin`: 9x5x9 Furnished Oak & Cobblestone Log Cabin with Roof, Windows, Doorway,
      Lanterns, 4 Beds, Crafting/Smelting/Anvil Stations, and 2 Stocked Supply Chests
    - `stone_fortress`: 11x6x11 Fortified Stone Brick Keep with Roof Battlements, Ladder,
      Enchanting Corner, Workstations, 4 Beds, and 2 Stocked Supply Chests
    """
    kind = (house_type or "none").lower().strip()
    if kind == "none":
        return []

    cmds: list[str] = ["changesetting allow-cheats true"]
    ex = lambda c: f"execute at {at_selector} run {c}"

    if kind == "camp_pad":
        cmds.extend(
            [
                ex("fill ~3 ~0 ~-2 ~7 ~4 ~2 air"),
                ex("fill ~3 ~-1 ~-2 ~7 ~-1 ~2 cobblestone"),
                ex("fill ~4 ~-1 ~-1 ~6 ~-1 ~1 oak_planks"),
                # Corner Fence & Lantern Posts
                ex("setblock ~3 ~0 ~-2 oak_fence"),
                ex("setblock ~3 ~1 ~-2 lantern"),
                ex("setblock ~7 ~0 ~-2 oak_fence"),
                ex("setblock ~7 ~1 ~-2 lantern"),
                ex("setblock ~3 ~0 ~2 oak_fence"),
                ex("setblock ~3 ~1 ~2 lantern"),
                ex("setblock ~7 ~0 ~2 oak_fence"),
                ex("setblock ~7 ~1 ~2 lantern"),
                # Workstations & Beds
                ex("setblock ~6 ~0 ~-1 crafting_table"),
                ex("setblock ~6 ~0 ~0 furnace"),
                ex("setblock ~6 ~0 ~1 blast_furnace"),
                ex("setblock ~5 ~0 ~-2 bed"),
                ex("setblock ~5 ~0 ~2 bed"),
                ex("setblock ~4 ~0 ~0 chest"),
            ]
        )
        cmds.extend(
            _stock_chest_commands(
                "~4 ~0 ~0",
                [
                    (0, "bed", 1),
                    (1, "bed", 1),
                    (2, "iron_sword", 2),
                    (3, "iron_pickaxe", 2),
                    (4, "iron_axe", 2),
                    (5, "shield", 2),
                    (6, "cooked_beef", 64),
                    (7, "torch", 64),
                    (8, "oak_planks", 64),
                    (9, "cobblestone", 64),
                ],
                at_selector,
            )
        )
        return cmds

    if kind == "coop_cabin":
        # 9x5x9 Furnished Co-Op Log Cabin from X: ~4..~12, Y: ~-1..~4, Z: ~-4..~4
        cmds.extend(
            [
                # Clear interior + porch volume first
                ex("fill ~3 ~0 ~-4 ~12 ~5 ~4 air"),
                # Cobblestone Foundation & Front Porch Step
                ex("fill ~3 ~-1 ~-4 ~12 ~-1 ~4 cobblestone"),
                # Hollow Oak Plank Cabin Box
                ex("fill ~4 ~0 ~-4 ~12 ~4 ~4 oak_planks outline"),
                # Pitched / Slab Roof Cap & Cobblestone Trim
                ex("fill ~4 ~4 ~-4 ~12 ~4 ~4 cobblestone"),
                ex("fill ~5 ~4 ~-3 ~11 ~4 ~3 oak_planks"),
                # 4 Oak Log Corner Pillars
                ex("fill ~4 ~0 ~-4 ~4 ~4 ~-4 oak_log"),
                ex("fill ~12 ~0 ~-4 ~12 ~4 ~-4 oak_log"),
                ex("fill ~4 ~0 ~4 ~4 ~4 ~4 oak_log"),
                ex("fill ~12 ~0 ~4 ~12 ~4 ~4 oak_log"),
                # Glass Windows on North, South, East, and Front Walls
                ex("fill ~7 ~1 ~-4 ~9 ~2 ~-4 glass"),
                ex("fill ~7 ~1 ~4 ~9 ~2 ~4 glass"),
                ex("fill ~12 ~1 ~-1 ~12 ~2 ~1 glass"),
                ex("setblock ~4 ~1 ~-2 glass"),
                ex("setblock ~4 ~2 ~-2 glass"),
                ex("setblock ~4 ~1 ~2 glass"),
                ex("setblock ~4 ~2 ~2 glass"),
                # Front Entryway (2-high opening facing player at X=~4, Z=0) + Porch Lanterns
                ex("fill ~4 ~0 ~0 ~4 ~1 ~0 air"),
                ex("setblock ~3 ~2 ~-1 torch"),
                ex("setblock ~3 ~2 ~1 torch"),
                # Interior Ceiling Lanterns (Mob-proof illumination)
                ex("setblock ~6 ~3 ~-2 lantern"),
                ex("setblock ~6 ~3 ~2 lantern"),
                ex("setblock ~10 ~3 ~-2 lantern"),
                ex("setblock ~10 ~3 ~2 lantern"),
                # Co-Op Bedroom Wall (North side: Beds)
                ex("setblock ~6 ~0 ~-3 bed"),
                ex("setblock ~8 ~0 ~-3 bed"),
                ex("setblock ~10 ~0 ~-3 bed"),
                # Crafting, Smelting & Forging Wall (East side: X=~11)
                ex("setblock ~11 ~0 ~-2 crafting_table"),
                ex("setblock ~11 ~0 ~-1 furnace"),
                ex("setblock ~11 ~0 ~0 blast_furnace"),
                ex("setblock ~11 ~0 ~1 smoker"),
                ex("setblock ~11 ~0 ~2 anvil"),
                ex("setblock ~11 ~0 ~3 grindstone"),
                # Storage Wall (South side: Stocked Chest + Stocked Barrel)
                ex("setblock ~6 ~0 ~3 chest"),
                ex("setblock ~8 ~0 ~3 barrel"),
                ex("setblock ~10 ~0 ~3 cartography_table"),
            ]
        )
        # Stock Cabin Chest (`~6 ~0 ~3`) with Co-Op Weapons, Tools, Armor, Beds & Food
        cmds.extend(
            _stock_chest_commands(
                "~6 ~0 ~3",
                [
                    (0, "bed", 1),
                    (1, "bed", 1),
                    (2, "bed", 1),
                    (3, "bed", 1),
                    (4, "iron_sword", 1),
                    (5, "iron_sword", 1),
                    (6, "iron_pickaxe", 1),
                    (7, "iron_pickaxe", 1),
                    (8, "iron_axe", 1),
                    (9, "iron_axe", 1),
                    (10, "iron_shovel", 1),
                    (11, "shield", 1),
                    (12, "shield", 1),
                    (13, "bow", 1),
                    (14, "arrow", 64),
                    (15, "iron_helmet", 1),
                    (16, "iron_chestplate", 1),
                    (17, "iron_leggings", 1),
                    (18, "iron_boots", 1),
                    (19, "cooked_beef", 64),
                    (20, "bread", 64),
                    (21, "golden_apple", 16),
                    (22, "torch", 64),
                    (23, "water_bucket", 1),
                    (24, "empty_map", 4),
                    (25, "compass", 4),
                    (26, "lantern", 16),
                ],
                at_selector,
            )
        )
        # Stock Cabin Barrel (`~8 ~0 ~3`) with Building Materials & Enchanting Supplies
        cmds.extend(
            _stock_chest_commands(
                "~8 ~0 ~3",
                [
                    (0, "oak_planks", 64),
                    (1, "oak_planks", 64),
                    (2, "oak_planks", 64),
                    (3, "oak_planks", 64),
                    (4, "cobblestone", 64),
                    (5, "cobblestone", 64),
                    (6, "cobblestone", 64),
                    (7, "cobblestone", 64),
                    (8, "oak_log", 64),
                    (9, "stone_bricks", 64),
                    (10, "glass", 64),
                    (11, "oak_stairs", 64),
                    (12, "oak_fence", 64),
                    (13, "wooden_door", 4),
                    (14, "enchanting_table", 1),
                    (15, "bookshelf", 15),
                ],
                at_selector,
            )
        )
        return cmds

    if kind == "stone_fortress":
        # 11x6x11 Fortified Stone Brick Keep from X: ~4..~14, Y: ~-1..~5, Z: ~-5..~5
        cmds.extend(
            [
                ex("fill ~3 ~0 ~-5 ~14 ~6 ~5 air"),
                ex("fill ~3 ~-1 ~-5 ~14 ~-1 ~5 stone_bricks"),
                ex("fill ~4 ~0 ~-5 ~14 ~4 ~5 stone_bricks outline"),
                # Roof Battlements (Crenellations)
                ex("fill ~4 ~5 ~-5 ~14 ~5 ~5 stone_bricks outline"),
                ex("fill ~5 ~5 ~-4 ~13 ~5 ~4 air"),
                # Glass Windows
                ex("fill ~8 ~1 ~-5 ~10 ~2 ~-5 glass"),
                ex("fill ~8 ~1 ~5 ~10 ~2 ~5 glass"),
                ex("fill ~14 ~1 ~-1 ~14 ~2 ~1 glass"),
                # Front Gate Opening + Lanterns
                ex("fill ~4 ~0 ~0 ~4 ~2 ~0 air"),
                ex("setblock ~3 ~2 ~-1 lantern"),
                ex("setblock ~3 ~2 ~1 lantern"),
                # Interior Lighting
                ex("setblock ~6 ~3 ~-3 lantern"),
                ex("setblock ~6 ~3 ~3 lantern"),
                ex("setblock ~12 ~3 ~-3 lantern"),
                ex("setblock ~12 ~3 ~3 lantern"),
                # Enchanting Library Corner & Workstations
                ex("setblock ~12 ~0 ~-3 enchanting_table"),
                ex("fill ~13 ~0 ~-4 ~13 ~1 ~-2 bookshelf"),
                ex("setblock ~13 ~0 ~0 crafting_table"),
                ex("setblock ~13 ~0 ~1 furnace"),
                ex("setblock ~13 ~0 ~2 blast_furnace"),
                ex("setblock ~13 ~0 ~3 smoker"),
                ex("setblock ~13 ~0 ~4 anvil"),
                # Beds & Stocked Chest/Barrel
                ex("setblock ~6 ~0 ~-4 bed"),
                ex("setblock ~8 ~0 ~-4 bed"),
                ex("setblock ~10 ~0 ~-4 bed"),
                ex("setblock ~6 ~0 ~4 chest"),
                ex("setblock ~8 ~0 ~4 barrel"),
            ]
        )
        cmds.extend(
            _stock_chest_commands(
                "~6 ~0 ~4",
                [
                    (0, "bed", 4),
                    (1, "diamond_sword", 1),
                    (2, "iron_sword", 3),
                    (3, "iron_pickaxe", 4),
                    (4, "iron_axe", 4),
                    (5, "shield", 4),
                    (6, "bow", 2),
                    (7, "arrow", 64),
                    (8, "cooked_beef", 64),
                    (9, "golden_apple", 16),
                    (10, "torch", 64),
                    (11, "lantern", 32),
                    (12, "water_bucket", 2),
                    (13, "empty_map", 4),
                    (14, "stone_bricks", 64),
                    (15, "oak_planks", 64),
                ],
                at_selector,
            )
        )
        cmds.extend(
            _stock_chest_commands(
                "~8 ~0 ~4",
                [
                    (0, "stone_bricks", 64),
                    (1, "stone_bricks", 64),
                    (2, "cobblestone", 64),
                    (3, "cobblestone", 64),
                    (4, "oak_planks", 64),
                    (5, "oak_planks", 64),
                    (6, "oak_log", 64),
                    (7, "glass", 64),
                    (8, "iron_helmet", 4),
                    (9, "iron_chestplate", 4),
                    (10, "iron_leggings", 4),
                    (11, "iron_boots", 4),
                ],
                at_selector,
            )
        )
        return cmds

    return []

