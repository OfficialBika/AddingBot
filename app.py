import asyncio
import hashlib
import logging
import os
import re
import tempfile
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Optional

import cv2
import imagehash
from aiohttp import web
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    BotCommand,
    CopyTextButton,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from PIL import Image

load_dotenv()

# -----------------------------------------------------
# Config
# -----------------------------------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
MONGO_URI = os.getenv("MONGO_URI", "").strip()
DB_NAME = os.getenv("DB_NAME", "hallow_match_bot").strip()

OWNER_IDS = {
    int(x.strip())
    for x in os.getenv("OWNER_IDS", os.getenv("OWNER_ID", "")).split(",")
    if x.strip().isdigit()
}

PHOTO_PHASH_THRESHOLD = int(os.getenv("PHOTO_PHASH_THRESHOLD", "8"))
VIDEO_FRAME_THRESHOLD = int(os.getenv("VIDEO_FRAME_THRESHOLD", "10"))
VIDEO_AVG_THRESHOLD = int(os.getenv("VIDEO_AVG_THRESHOLD", "12"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
OWNER_USERNAME = os.getenv("OWNER_USERNAME", "@Official_Bika").strip()
DEFAULT_COMMAND = os.getenv("DEFAULT_COMMAND", "/hallow").strip() or "/hallow"
ADDED_LOG_CHANNEL = os.getenv("ADDED_LOG_CHANNEL", "@WaifuAddedList").strip()

DEFAULT_TARGET_CHAT_RAW = os.getenv("DEFAULT_TARGET_CHAT", "").strip()
DEFAULT_TARGET_CHAT = (
    int(DEFAULT_TARGET_CHAT_RAW)
    if DEFAULT_TARGET_CHAT_RAW and DEFAULT_TARGET_CHAT_RAW.lstrip("-").isdigit()
    else None
)

FORWARD_SOURCE_COMMANDS_RAW = os.getenv(
    "FORWARD_SOURCE_COMMANDS",
    "@CaptureDatabase:/capture,@Seizer_Database:/seize,CAPTURE|UPLOADS:/capture,SEIZER DATABASE:/seize",
).strip()

KNOWN_INLINE_SOURCE_COMMAND_MAP: dict[str, str] = {
    "character_catcher_bot": "/catch",
    "character_seizer_bot": "/seize",
    "capturecharacterbot": "/capture",
    "capture_character_bot": "/capture",
    "takers_character_bot": "/take",
    "grab_your_waifu_bot": "/grab",
}

INLINE_SOURCE_BOTS = set(KNOWN_INLINE_SOURCE_COMMAND_MAP.keys())
INLINE_SOURCE_BOTS.update(
    {
        x.strip().lstrip("@").lower()
        for x in os.getenv("INLINE_SOURCE_BOTS", "").split(",")
        if x.strip()
    }
)
INLINE_SOURCE_COMMAND_MAP: dict[str, str] = dict(KNOWN_INLINE_SOURCE_COMMAND_MAP)

USE_WEBHOOK = os.getenv("USE_WEBHOOK", "false").lower() == "true"
PUBLIC_URL = os.getenv("PUBLIC_URL", "").strip()
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook").strip() or "/webhook"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
PORT = int(os.getenv("PORT", "8080"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI is required")
if not OWNER_IDS:
    raise RuntimeError("OWNER_ID or OWNER_IDS is required")
if USE_WEBHOOK and (not PUBLIC_URL or not WEBHOOK_SECRET):
    raise RuntimeError("PUBLIC_URL and WEBHOOK_SECRET are required when USE_WEBHOOK=true")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("adding-bot")

# -----------------------------------------------------
# Database
# -----------------------------------------------------
client = AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]
items = db.items
sudo_users = db.sudo_users
known_users = db.known_users
user_modes = db.user_modes
settings_col = db.settings

# -----------------------------------------------------
# Helpers
# -----------------------------------------------------
NAME_PATTERNS = [
    re.compile(r"^[^\n\r]*?Character\s*Name\s*[:：﹕꞉-]?\s*(.+?)\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^[^\n\r]*?\bNAME\s*[:：﹕꞉-]?\s*(.+?)\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^[^\n\r]*?\bName\s*[:：﹕꞉-]?\s*(.+?)\s*$", re.IGNORECASE | re.MULTILINE),
]

ANIME_PATTERNS = [
    re.compile(r"^[^\n\r]*?Anime\s*Name\s*[:：﹕꞉-]?\s*(.+?)\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^[^\n\r]*?Anime\s*[:：﹕꞉-]?\s*(.+?)\s*$", re.IGNORECASE | re.MULTILINE),
]

RARITY_PATTERNS = [
    re.compile(r"^[^\n\r]*?Rarity\s*[:：﹕꞉-]?\s*(.+?)\s*$", re.IGNORECASE | re.MULTILINE),
]

CARD_ID_PATTERNS = [
    re.compile(r"^[^\n\r]*?Character\s*ID\s*[:：﹕꞉-]?\s*#?\s*([0-9]+)\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^[^\n\r]*?Card\s*ID\s*[:：﹕꞉-]?\s*#?\s*([0-9]+)\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^[^\n\r]*?ID\s*[:：﹕꞉-]?\s*#?\s*([0-9]+)\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^[^\n\r]*?Id\s*[:：﹕꞉-]?\s*#?\s*([0-9]+)\s*$", re.IGNORECASE | re.MULTILINE),
]

COMMAND_PATTERNS = [
    re.compile(r"(?:using|use|hint|full).*?/\s*([A-Za-z0-9_]+)\b", re.IGNORECASE | re.DOTALL),
    re.compile(r"/\s*([A-Za-z0-9_]+)\s*(?:\[[^\]]*name[^\]]*\]|\([^\)]*name[^\)]*\)|\bname\b)", re.IGNORECASE | re.DOTALL),
    re.compile(r"/\s*([A-Za-z0-9_]+)\b", re.IGNORECASE),
]

NUMBERED_NAME_RE = re.compile(
    r"^\s*(\d+)\s*:?[ \t]+(.+?)\s*(?:\[[^\]]*\]|\([^\)]*\))?\s*$",
    re.IGNORECASE | re.MULTILINE,
)
CHARACTER_CATCHER_HEADER_RE = re.compile(r"OwO!\s*Check out this character!", re.IGNORECASE)
SOURCE_NAME_PATTERNS = [
    re.compile(r"^[^\n\r]*?\bName\b\s*[:：﹕꞉-]?\s*(.+?)\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^[^\n\r]*?\bCharacter\s*Name\b\s*[:：﹕꞉-]?\s*(.+?)\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^[^\n\r]*?🍀\s*Name\s*[:：﹕꞉-]?\s*(.+?)\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^[^\n\r]*?👤\s*Name\s*[:：﹕꞉-]?\s*(.+?)\s*$", re.IGNORECASE | re.MULTILINE),
]
SOURCE_ANIME_PATTERNS = [
    re.compile(r"^[^\n\r]*?\bAnime\b\s*[:：﹕꞉-]?\s*(.+?)\s*$", re.IGNORECASE | re.MULTILINE),
]
SOURCE_RARITY_PATTERNS = [
    re.compile(r"^[^\n\r]*?\bRarity\b\s*[:：﹕꞉-]?\s*(.+?)\s*$", re.IGNORECASE | re.MULTILINE),
]
SOURCE_CARD_ID_PATTERNS = [
    re.compile(r"^[^\n\r]*?\bCharacter\s*ID\b\s*[:：﹕꞉-]?\s*#?\s*([0-9]+)\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^[^\n\r]*?\bID\b\s*[:：﹕꞉-]?\s*#?\s*([0-9]+)\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^[^\n\r]*?🆔\s*ID\s*[:：﹕꞉-]?\s*#?\s*([0-9]+)\s*$", re.IGNORECASE | re.MULTILINE),
]
TRAILING_BADGE_RE = re.compile(r"\s*\[([^\[\]]+)\]\s*$")

SUPPORTED_BOTS = [
    ("hallow", "@Characters_Hallow_bot", ["/hallow"]),
    ("catcher", "@Character_Catcher_Bot", ["/catch"]),
    ("seizer", "@Character_Seizer_Bot", ["/seize", "/sezer"]),
    ("capture", "@CaptureCharacterBot", ["/capture"]),
    ("takers", "@Takers_character_bot", ["/take"]),
    ("grab", "@Grab_Your_Waifu_Bot", ["/grab"]),
]

router = Router()


@dataclass
class MediaMeta:
    media_type: str
    file_id: str
    file_unique_id: str
    sha256: str
    phash: Optional[str] = None
    frame_hashes: Optional[list[str]] = None


@dataclass
class ParsedText:
    name: Optional[str]
    anime_name: Optional[str]
    rarity: Optional[str]
    card_id: Optional[str]
    command_name: Optional[str]
    raw_text: str


def html_escape(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def clean_value(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_name(name: str) -> str:
    return clean_value(name).casefold()


def normalize_parse_text(text: Optional[str]) -> str:
    text = text or ""
    text = unicodedata.normalize("NFKC", text)
    text = text.replace("\r", "\n")
    text = text.replace("：", ":").replace("﹕", ":").replace("꞉", ":")
    text = re.sub(r"[\u200b-\u200f\u2060\ufeff]", "", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def parse_field(text: str, patterns: list[re.Pattern]) -> Optional[str]:
    for pattern in patterns:
        match = pattern.search(text or "")
        if match:
            return clean_value(match.group(1))
    return None


def clean_command_name(value: str) -> str:
    cmd = clean_value(value)
    if not cmd:
        return DEFAULT_COMMAND
    if not cmd.startswith("/"):
        cmd = f"/{cmd.lstrip('/')}"
    return cmd.split()[0]


def normalize_forward_mapping_key(value: str) -> str:
    return clean_value(value).lstrip("@").casefold()


def strip_trailing_badge(value: str) -> str:
    value = clean_value(value)
    if not value:
        return value

    while True:
        match = TRAILING_BADGE_RE.search(value)
        if not match:
            break
        inner = match.group(1)
        if re.search(r"[A-Za-z0-9]", inner):
            break
        value = clean_value(value[: match.start()])

    return value


def strip_leading_symbols(value: str) -> str:
    value = clean_value(value)
    return re.sub(r"^[^\w\u00C0-\u024F\u0400-\u04FF\u3040-\u30FF\u4E00-\u9FFF\uAC00-\uD7AF]+", "", value).strip()


def strip_grab_name_suffix(value: str) -> str:
    value = clean_value(value)
    return clean_value(re.sub(r"\s*[-–—|]+\s*(?:rarity|anime|id)\b.*$", "", value, flags=re.IGNORECASE))


def finalize_parsed_text(parsed: ParsedText) -> ParsedText:
    parsed.name = strip_grab_name_suffix(strip_trailing_badge(strip_leading_symbols(parsed.name or ""))) or None
    parsed.anime_name = strip_trailing_badge(strip_leading_symbols(parsed.anime_name or "")) or None
    parsed.rarity = strip_leading_symbols(parsed.rarity or "") or None
    parsed.card_id = clean_value(parsed.card_id or "") or None
    if parsed.command_name:
        parsed.command_name = clean_command_name(parsed.command_name)
    parsed.raw_text = normalize_parse_text(parsed.raw_text)
    return parsed


FORWARD_SOURCE_USERNAME_COMMAND_MAP: dict[str, str] = {}
FORWARD_SOURCE_TITLE_COMMAND_MAP: dict[str, str] = {}


def register_forward_source_command(key: str, command_name: str) -> None:
    normalized_key = normalize_forward_mapping_key(key)
    if not normalized_key:
        return
    normalized_command = clean_command_name(command_name)
    if key.strip().startswith("@") or re.fullmatch(r"[A-Za-z0-9_]+", key.strip()):
        FORWARD_SOURCE_USERNAME_COMMAND_MAP[normalized_key] = normalized_command
    else:
        FORWARD_SOURCE_TITLE_COMMAND_MAP[normalized_key] = normalized_command


for _mapping in [x.strip() for x in FORWARD_SOURCE_COMMANDS_RAW.split(",") if x.strip() and ":" in x]:
    _key, _cmd = _mapping.split(":", 1)
    register_forward_source_command(_key, _cmd)


def parse_command_name(text: str) -> Optional[str]:
    raw = unicodedata.normalize("NFKC", normalize_parse_text(text or ""))
    for pattern in COMMAND_PATTERNS:
        match = pattern.search(raw)
        if match:
            return clean_command_name("/" + match.group(1))
    return None


def parse_caption_text(text: Optional[str]) -> ParsedText:
    raw = normalize_parse_text(text)
    return finalize_parsed_text(
        ParsedText(
            name=parse_field(raw, NAME_PATTERNS),
            anime_name=parse_field(raw, ANIME_PATTERNS),
            rarity=parse_field(raw, RARITY_PATTERNS),
            card_id=parse_field(raw, CARD_ID_PATTERNS),
            command_name=parse_command_name(raw),
            raw_text=raw,
        )
    )


def collect_candidate_texts(message: Message) -> list[str]:
    candidates: list[str] = []
    for value in [getattr(message, "caption", None), getattr(message, "text", None)]:
        value = normalize_parse_text(value)
        if value and value not in candidates:
            candidates.append(value)
    ext = getattr(message, "external_reply", None)
    if ext is not None:
        for value in [getattr(ext, "caption", None), getattr(ext, "text", None)]:
            value = normalize_parse_text(value)
            if value and value not in candidates:
                candidates.append(value)
    return candidates


def parse_caption_text_from_message(message: Message) -> ParsedText:
    candidates = collect_candidate_texts(message)
    for raw in candidates:
        parsed = parse_caption_text(raw)
        if parsed.name:
            return parsed
    raw = candidates[0] if candidates else ""
    return parse_caption_text(raw)


def get_combined_message_text(message: Message) -> str:
    return "\n".join(collect_candidate_texts(message)).strip()


def extract_media_handle(message: Message):
    if message.photo:
        return "photo", message.photo[-1]
    if message.video:
        return "video", message.video
    return None, None


def is_group_chat(message: Message) -> bool:
    return bool(message.chat and getattr(message.chat, "type", "") in {"group", "supergroup"})


def is_private_chat(message: Message) -> bool:
    return bool(message.chat and getattr(message.chat, "type", "") == "private")


def is_default_target_chat(message: Message) -> bool:
    return bool(DEFAULT_TARGET_CHAT is not None and message.chat and message.chat.id == DEFAULT_TARGET_CHAT)


def get_inline_source_username(message: Message) -> str:
    via_bot = getattr(message, "via_bot", None)
    if via_bot is None:
        return ""
    return (getattr(via_bot, "username", "") or "").lower().strip()


def get_inline_source_command(message: Message) -> Optional[str]:
    return INLINE_SOURCE_COMMAND_MAP.get(get_inline_source_username(message))


def is_character_catcher_style_message(message: Message) -> bool:
    raw = get_combined_message_text(message)
    return bool(raw and CHARACTER_CATCHER_HEADER_RE.search(raw) and NUMBERED_NAME_RE.search(raw))


def infer_anime_from_lines(lines: list[str], match_line_index: int) -> Optional[str]:
    if match_line_index <= 0:
        return None
    for i in range(match_line_index - 1, -1, -1):
        line = clean_value(lines[i])
        if not line:
            continue
        if CHARACTER_CATCHER_HEADER_RE.search(line):
            continue
        if re.search(r"\b(?:rarity|added\s*by|price|id|character\s*id)\b", line, re.IGNORECASE):
            continue
        if re.search(r"new\s+(?:character|waifu)\s+added", line, re.IGNORECASE):
            continue
        return strip_trailing_badge(strip_leading_symbols(line)) or None
    return None


def parse_numbered_name_message(message: Message, forced_command: str) -> ParsedText:
    raw = get_combined_message_text(message)
    lines = [clean_value(x) for x in raw.splitlines() if clean_value(x)]
    name = None
    anime_name = None
    card_id = None
    match = NUMBERED_NAME_RE.search(raw)
    if match:
        card_id = clean_value(match.group(1))
        name = clean_value(match.group(2))
        match_line = clean_value(match.group(0))
        if match_line in lines:
            anime_name = infer_anime_from_lines(lines, lines.index(match_line))
    parsed = ParsedText(
        name=name,
        anime_name=anime_name,
        rarity=parse_field(raw, SOURCE_RARITY_PATTERNS) or parse_field(raw, RARITY_PATTERNS),
        card_id=card_id,
        command_name=forced_command,
        raw_text=raw,
    )
    return finalize_parsed_text(parsed)


def parse_grab_inline_message(message: Message, forced_command: str) -> ParsedText:
    raw = get_combined_message_text(message)
    parsed = ParsedText(
        name=parse_field(raw, SOURCE_NAME_PATTERNS) or parse_field(raw, NAME_PATTERNS),
        anime_name=parse_field(raw, SOURCE_ANIME_PATTERNS) or parse_field(raw, ANIME_PATTERNS),
        rarity=parse_field(raw, SOURCE_RARITY_PATTERNS) or parse_field(raw, RARITY_PATTERNS),
        card_id=parse_field(raw, SOURCE_CARD_ID_PATTERNS) or parse_field(raw, CARD_ID_PATTERNS),
        command_name=forced_command,
        raw_text=raw,
    )
    return finalize_parsed_text(parsed)


def is_forwarded_message(message: Message) -> bool:
    return bool(
        getattr(message, "forward_origin", None)
        or getattr(message, "forward_from_chat", None)
        or getattr(message, "forward_from", None)
        or getattr(message, "forward_sender_name", None)
    )


def get_forward_source_info(message: Message) -> dict[str, Any]:
    info: dict[str, Any] = {"chat_id": None, "username": "", "title": "", "origin_type": ""}
    origin = getattr(message, "forward_origin", None)
    if origin:
        info["origin_type"] = origin.__class__.__name__
        chat = getattr(origin, "chat", None)
        if chat is None:
            sender_chat = getattr(origin, "sender_chat", None)
            if sender_chat is not None:
                chat = sender_chat
        if chat is not None:
            info["chat_id"] = getattr(chat, "id", None)
            info["username"] = (getattr(chat, "username", "") or "").lower()
            info["title"] = clean_value(getattr(chat, "title", "") or "").casefold()
            return info
        sender_user_name = (getattr(origin, "sender_user_name", "") or "").lower()
        if sender_user_name:
            info["username"] = sender_user_name
            return info
    legacy_chat = getattr(message, "forward_from_chat", None)
    if legacy_chat is not None:
        info["chat_id"] = getattr(legacy_chat, "id", None)
        info["username"] = (getattr(legacy_chat, "username", "") or "").lower()
        info["title"] = clean_value(getattr(legacy_chat, "title", "") or "").casefold()
        info["origin_type"] = "legacy_forward_chat"
        return info
    sender_chat = getattr(message, "sender_chat", None)
    if sender_chat is not None:
        info["chat_id"] = getattr(sender_chat, "id", None)
        info["username"] = (getattr(sender_chat, "username", "") or "").lower()
        info["title"] = clean_value(getattr(sender_chat, "title", "") or "").casefold()
        info["origin_type"] = "sender_chat"
        return info
    return info


def get_forward_source_command(message: Message) -> Optional[str]:
    if not is_forwarded_message(message):
        return None
    info = get_forward_source_info(message)
    username = normalize_forward_mapping_key(info.get("username", ""))
    title = normalize_forward_mapping_key(info.get("title", ""))
    if username and username in FORWARD_SOURCE_USERNAME_COMMAND_MAP:
        return FORWARD_SOURCE_USERNAME_COMMAND_MAP[username]
    if title and title in FORWARD_SOURCE_TITLE_COMMAND_MAP:
        return FORWARD_SOURCE_TITLE_COMMAND_MAP[title]
    for key, command_name in FORWARD_SOURCE_TITLE_COMMAND_MAP.items():
        if key and title and (key in title or title in key):
            return command_name
    return None


def is_allowed_forward_source(message: Message) -> bool:
    return is_forwarded_message(message) and bool(get_forward_source_command(message))


def parse_forward_source_message(message: Message, forced_command: str) -> ParsedText:
    raw = get_combined_message_text(message)
    numbered = parse_numbered_name_message(message, forced_command)
    if numbered.name:
        return numbered
    parsed = ParsedText(
        name=parse_field(raw, SOURCE_NAME_PATTERNS) or parse_field(raw, NAME_PATTERNS),
        anime_name=parse_field(raw, SOURCE_ANIME_PATTERNS) or parse_field(raw, ANIME_PATTERNS),
        rarity=parse_field(raw, SOURCE_RARITY_PATTERNS) or parse_field(raw, RARITY_PATTERNS),
        card_id=parse_field(raw, SOURCE_CARD_ID_PATTERNS) or parse_field(raw, CARD_ID_PATTERNS),
        command_name=forced_command,
        raw_text=raw,
    )
    return finalize_parsed_text(parsed)


def get_effective_parsed_message(message: Message) -> ParsedText:
    parsed = parse_caption_text_from_message(message)
    inline_cmd = get_inline_source_command(message)
    if inline_cmd:
        if inline_cmd == "/grab":
            grab_parsed = parse_grab_inline_message(message, inline_cmd)
            if grab_parsed.name or grab_parsed.card_id:
                return grab_parsed
        inline_parsed = parse_numbered_name_message(message, inline_cmd)
        if inline_parsed.name:
            return inline_parsed
        parsed.command_name = inline_cmd
        return finalize_parsed_text(parsed)

    forward_cmd = get_forward_source_command(message)
    if forward_cmd:
        forward_parsed = parse_forward_source_message(message, forward_cmd)
        if forward_parsed.name or forward_parsed.card_id:
            return forward_parsed
        parsed.command_name = forward_cmd
        return finalize_parsed_text(parsed)

    if is_character_catcher_style_message(message):
        cc_parsed = parse_numbered_name_message(message, "/catch")
        if cc_parsed.name:
            return cc_parsed
        parsed.command_name = "/catch"
        return finalize_parsed_text(parsed)

    if is_allowed_forward_source(message):
        parsed.command_name = get_forward_source_command(message)
        return finalize_parsed_text(parsed)

    return finalize_parsed_text(parsed)


def get_autosave_source_label(message: Message) -> str:
    inline_username = get_inline_source_username(message)
    if inline_username:
        return f"inline @{inline_username}"
    if is_character_catcher_style_message(message):
        return "Character_Catcher style"
    source_info = get_forward_source_info(message)
    return source_info.get("title") or source_info.get("username") or str(source_info.get("chat_id") or "forwarded source")


def get_log_source_label(message: Message) -> str:
    if get_inline_source_command(message):
        return get_autosave_source_label(message)
    if is_character_catcher_style_message(message):
        return get_autosave_source_label(message)
    if is_allowed_forward_source(message):
        return get_autosave_source_label(message)
    if is_forwarded_message(message):
        return get_autosave_source_label(message)
    return "manual-save"


def build_user_mention_html(user) -> str:
    if not user:
        return "Unknown"
    username = (getattr(user, "username", "") or "").strip()
    full_name = clean_value(getattr(user, "full_name", "") or username or "Unknown")
    if username:
        return f'<a href="https://t.me/{html_escape(username)}">{html_escape(full_name)}</a>'
    user_id = getattr(user, "id", None)
    if user_id:
        return f'<a href="tg://user?id={user_id}">{html_escape(full_name)}</a>'
    return html_escape(full_name)


def build_added_log_caption(*, doc: dict[str, Any], created: bool, mode: str, source_label: str, added_by_user) -> str:
    status = "Saved" if created else "Updated"
    name = clean_value(doc.get("name") or "Unknown")
    card_id = clean_value(doc.get("card_id") or "-")
    cmd = clean_command_name(doc.get("command_name") or DEFAULT_COMMAND)
    added_by = build_user_mention_html(added_by_user)
    lines = [
        f"{status}",
        f"Name : {html_escape(name)}",
        f"ID : {html_escape(card_id)}",
        f"Mode: {html_escape(mode)}",
        f"Source: {html_escape(source_label)}",
        f"Cmd: <code>{html_escape(cmd)}</code>",
        "",
        f"Added by {added_by}",
    ]
    return "\n".join(lines)


async def send_added_log(*, bot: Bot, source_message: Message, doc: dict[str, Any], created: bool, mode: str, source_label: str, added_by_user) -> None:
    if not ADDED_LOG_CHANNEL:
        return
    media_type, media = extract_media_handle(source_message)
    if not media_type or not media:
        return
    caption = build_added_log_caption(
        doc=doc,
        created=created,
        mode=mode,
        source_label=source_label,
        added_by_user=added_by_user,
    )
    if media_type == "photo":
        await bot.send_photo(chat_id=ADDED_LOG_CHANNEL, photo=media.file_id, caption=caption, parse_mode=ParseMode.HTML)
    else:
        await bot.send_video(chat_id=ADDED_LOG_CHANNEL, video=media.file_id, caption=caption, parse_mode=ParseMode.HTML)


def get_source_bot_key_from_command(command_name: str) -> str:
    mapping = {
        "/hallow": "hallow",
        "/catch": "catcher",
        "/seize": "seizer",
        "/sezer": "seizer",
        "/capture": "capture",
        "/take": "takers",
        "/grab": "grab",
    }
    return mapping.get(clean_command_name(command_name), "unknown")


async def count_media_for_bot_key(key: str, commands: list[str]) -> int:
    return await items.count_documents({"$or": [{"source_bot_key": key}, {"command_name": {"$in": commands}}]})


async def build_status_text() -> str:
    total_media = await items.count_documents({})
    photos = await items.count_documents({"media_type": "photo"})
    videos = await items.count_documents({"media_type": "video"})
    sudo_count = await sudo_users.count_documents({})
    users = await known_users.count_documents({})
    per_bot = []
    for _idx, (key, bot_username, commands) in enumerate(SUPPORTED_BOTS, start=1):
        count = await count_media_for_bot_key(key, commands)
        per_bot.append(f"{_idx}. {html_escape(bot_username)} : <b>{count}</b>")
    lines = [
        "🛠 <b>ADDING BOT STATUS</b>",
        f"‣ Total Media : <b>{total_media}</b>",
        f"‣ Photos : <b>{photos}</b>",
        f"‣ Videos : <b>{videos}</b>",
        f"‣ Known Users : <b>{users}</b>",
        f"‣ Sudo Users : <b>{sudo_count}</b>",
        f"‣ Target Chat : <code>{html_escape(str(DEFAULT_TARGET_CHAT or '-'))}</code>",
        f"‣ Added Log Channel : <code>{html_escape(ADDED_LOG_CHANNEL or '-')}</code>",
        f"‣ Mode : <b>{'WEBHOOK' if USE_WEBHOOK else 'POLLING'}</b>",
        "",
        "🤖 <b>Supported Add Sources</b>",
        *per_bot,
    ]
    return "\n".join(lines)


async def ensure_indexes() -> None:
    await items.create_index("file_unique_id", unique=True, sparse=True)
    await items.create_index("sha256", unique=True, sparse=True)
    await items.create_index("media_type")
    await items.create_index("normalized_name")
    await items.create_index("command_name")
    await items.create_index("source_bot_key")
    await items.create_index("created_at")
    await sudo_users.create_index("user_id", unique=True)
    await known_users.create_index("user_id", unique=True)
    await known_users.create_index("username")
    await user_modes.create_index("user_id", unique=True)
    await settings_col.create_index("key", unique=True)


async def remember_user(message: Message) -> None:
    user = message.from_user
    if not user:
        return
    await known_users.update_one(
        {"user_id": user.id},
        {"$set": {"user_id": user.id, "username": (user.username or "").lower(), "full_name": clean_value(user.full_name), "updated_at": datetime.now(timezone.utc)}},
        upsert=True,
    )


async def remember_chat(message: Message) -> None:
    await remember_user(message)


async def is_sudo_user(user_id: Optional[int]) -> bool:
    return bool(user_id and await sudo_users.find_one({"user_id": user_id}, {"_id": 1}))


async def can_save(message: Message) -> bool:
    user_id = message.from_user.id if message.from_user else None
    if not user_id:
        return False
    if user_id in OWNER_IDS:
        return True
    return await is_sudo_user(user_id)


async def set_autosave_mode(user_id: int, enabled: bool) -> None:
    await user_modes.update_one(
        {"user_id": user_id},
        {"$set": {"user_id": user_id, "autosave_enabled": enabled, "updated_at": datetime.now(timezone.utc)}},
        upsert=True,
    )


async def get_autosave_mode(user_id: Optional[int]) -> bool:
    if not user_id:
        return False
    row = await user_modes.find_one({"user_id": user_id})
    return bool(row and row.get("autosave_enabled"))


async def set_target_chat_autosave_mode(chat_id: int, enabled: bool, updated_by: int) -> None:
    await settings_col.update_one(
        {"key": f"target_chat_autosave:{chat_id}"},
        {"$set": {"key": f"target_chat_autosave:{chat_id}", "chat_id": chat_id, "enabled": enabled, "updated_by": updated_by, "updated_at": datetime.now(timezone.utc)}},
        upsert=True,
    )


async def get_target_chat_autosave_mode(chat_id: Optional[int]) -> bool:
    if not chat_id:
        return False
    row = await settings_col.find_one({"key": f"target_chat_autosave:{chat_id}"})
    return bool(row and row.get("enabled"))


async def upsert_item(*, meta: MediaMeta, parsed: ParsedText, saved_by: int) -> tuple[dict[str, Any], bool]:
    command_name = clean_command_name(parsed.command_name or DEFAULT_COMMAND)
    source_bot_key = get_source_bot_key_from_command(command_name)
    doc = {
        "name": clean_value(parsed.name or ""),
        "normalized_name": normalize_name(parsed.name or ""),
        "anime_name": clean_value(parsed.anime_name or ""),
        "rarity": clean_value(parsed.rarity or ""),
        "card_id": clean_value(parsed.card_id or ""),
        "command_name": command_name,
        "source_bot_key": source_bot_key,
        "raw_text": parsed.raw_text,
        "media_type": meta.media_type,
        "file_id": meta.file_id,
        "file_unique_id": meta.file_unique_id,
        "sha256": meta.sha256,
        "phash": meta.phash,
        "frame_hashes": meta.frame_hashes,
        "saved_by": saved_by,
        "updated_at": datetime.now(timezone.utc),
    }
    existing = await items.find_one({"$or": [{"file_unique_id": meta.file_unique_id}, {"sha256": meta.sha256}]})
    if existing:
        await items.update_one({"_id": existing["_id"]}, {"$set": doc})
        existing.update(doc)
        return existing, False
    doc["created_at"] = datetime.now(timezone.utc)
    result = await items.insert_one(doc)
    doc["_id"] = result.inserted_id
    return doc, True


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


async def download_file_bytes(bot: Bot, file_id: str) -> bytes:
    tg_file = await bot.get_file(file_id)
    if not tg_file.file_path:
        raise RuntimeError("Telegram did not return file_path")
    buffer = BytesIO()
    await bot.download_file(tg_file.file_path, destination=buffer)
    return buffer.getvalue()


def compute_photo_phash(data: bytes) -> str:
    with Image.open(BytesIO(data)) as img:
        img = img.convert("RGB")
        return str(imagehash.phash(img))


def _frame_to_hash(frame) -> str:
    rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    image = Image.fromarray(rgb)
    return str(imagehash.phash(image))


def compute_video_hashes(data: bytes) -> list[str]:
    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=True) as tmp:
        tmp.write(data)
        tmp.flush()
        cap = cv2.VideoCapture(tmp.name)
        if not cap.isOpened():
            raise RuntimeError("Failed to open video")
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        if frame_count <= 0:
            cap.release()
            raise RuntimeError("Video contains no readable frames")
        targets = sorted({max(0, int(frame_count * 0.2) - 1), max(0, int(frame_count * 0.5) - 1), max(0, int(frame_count * 0.8) - 1)})
        hashes: list[str] = []
        for idx in targets:
            cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
            ok, frame = cap.read()
            if ok and frame is not None:
                hashes.append(_frame_to_hash(frame))
        cap.release()
        if not hashes:
            raise RuntimeError("Could not extract frames from video")
        return hashes


async def get_media_meta(bot: Bot, message: Message) -> MediaMeta:
    media_type, media = extract_media_handle(message)
    if not media_type or not media:
        raise ValueError("Message does not contain supported media")
    raw = await download_file_bytes(bot, media.file_id)
    digest = sha256_hex(raw)
    if media_type == "photo":
        return MediaMeta(media_type="photo", file_id=media.file_id, file_unique_id=media.file_unique_id, sha256=digest, phash=compute_photo_phash(raw))
    return MediaMeta(media_type="video", file_id=media.file_id, file_unique_id=media.file_unique_id, sha256=digest, frame_hashes=compute_video_hashes(raw))


async def resolve_user_reference(message: Message, bot: Bot, raw_arg: Optional[str]) -> Optional[dict[str, Any]]:
    if message.reply_to_message and message.reply_to_message.from_user:
        target = message.reply_to_message.from_user
        return {"user_id": target.id, "username": (target.username or "").lower(), "full_name": clean_value(target.full_name)}
    arg = clean_value(raw_arg or "")
    if not arg:
        return None
    if arg.isdigit():
        known = await known_users.find_one({"user_id": int(arg)})
        return known or {"user_id": int(arg), "username": "", "full_name": ""}
    if arg.startswith("@"):
        username = arg.lstrip("@").lower()
        known = await known_users.find_one({"username": username})
        if known:
            return known
        try:
            chat = await bot.get_chat(arg)
            return {"user_id": chat.id, "username": (getattr(chat, "username", "") or "").lower(), "full_name": clean_value(getattr(chat, "full_name", "") or "")}
        except Exception:
            return None
    return None


def format_target_user(user_doc: dict[str, Any]) -> str:
    username = user_doc.get("username") or ""
    user_id = user_doc.get("user_id")
    full_name = clean_value(user_doc.get("full_name") or "")
    if username:
        return f"@{username} ({user_id})"
    if full_name:
        return f"{full_name} ({user_id})"
    return str(user_id)


async def set_access(collection, user_doc: dict[str, Any], added_by: int, enabled: bool) -> None:
    if enabled:
        await collection.update_one(
            {"user_id": user_doc["user_id"]},
            {"$set": {"user_id": user_doc["user_id"], "username": user_doc.get("username", ""), "full_name": user_doc.get("full_name", ""), "updated_at": datetime.now(timezone.utc), "updated_by": added_by}},
            upsert=True,
        )
    else:
        await collection.delete_one({"user_id": user_doc["user_id"]})


def build_start_text() -> str:
    return (
        "🛠 Adding Bot Ready\n\n"
        "ဒီ bot က media save / update only အတွက် သီးသန့်ဖြစ်ပါတယ်။\n\n"
        "Available:\n"
        "• /autosave on|off|status\n"
        "• /save (reply media)\n"
        "• /status\n"
        "• /stats\n"
        "• /addsudo /rmsudo\n\n"
        "Target chat မှာ save-only mode သုံးမယ်ဆို /autosave on လုပ်ပါ။"
    )


@router.message(Command("start"))
async def start_handler(message: Message) -> None:
    await remember_chat(message)
    if not await can_save(message):
        await message.reply("ဒီ adding bot ကို owner / sudo only သုံးလို့ရပါတယ်။")
        return
    await message.reply(build_start_text())


@router.message(Command("status"))
async def status_handler(message: Message) -> None:
    await remember_chat(message)
    if not await can_save(message):
        return
    await message.reply(await build_status_text(), parse_mode=ParseMode.HTML)


@router.message(Command("stats"))
async def stats_handler(message: Message) -> None:
    await remember_chat(message)
    if not await can_save(message):
        return
    await message.reply(await build_status_text(), parse_mode=ParseMode.HTML)


@router.message(Command("addsudo"))
async def addsudo_handler(message: Message, command: CommandObject, bot: Bot) -> None:
    await remember_chat(message)
    if not message.from_user or message.from_user.id not in OWNER_IDS:
        return
    target = await resolve_user_reference(message, bot, command.args)
    if not target:
        await message.reply("အသုံးပြုပုံ:\nReply + /addsudo\n/addsudo @username\n/addsudo 123456789")
        return
    await set_access(sudo_users, target, message.from_user.id, True)
    await message.reply(f"Sudo added: <b>{html_escape(format_target_user(target))}</b>", parse_mode=ParseMode.HTML)


@router.message(Command("rmsudo"))
async def rmsudo_handler(message: Message, command: CommandObject, bot: Bot) -> None:
    await remember_chat(message)
    if not message.from_user or message.from_user.id not in OWNER_IDS:
        return
    target = await resolve_user_reference(message, bot, command.args)
    if not target:
        await message.reply("အသုံးပြုပုံ:\nReply + /rmsudo\n/rmsudo @username\n/rmsudo 123456789")
        return
    await set_access(sudo_users, target, message.from_user.id, False)
    await message.reply(f"Sudo removed: <b>{html_escape(format_target_user(target))}</b>", parse_mode=ParseMode.HTML)


@router.message(Command("autosave"))
async def autosave_handler(message: Message, command: CommandObject) -> None:
    await remember_chat(message)
    if not (is_private_chat(message) or is_default_target_chat(message)):
        await message.reply("ဒီ command ကို DM/private chat (or) target chat ထဲမှာပဲ သုံးပါ။")
        return
    if not await can_save(message):
        return
    arg = clean_value(command.args or "").lower()
    if arg not in {"on", "off", "status"}:
        await message.reply("အသုံးပြုပုံ:\n/autosave on\n/autosave off\n/autosave status")
        return
    if is_default_target_chat(message):
        chat_id = message.chat.id
        if arg == "status":
            enabled = await get_target_chat_autosave_mode(chat_id)
            await message.reply(f"Target Chat Auto-save mode: <b>{'ON' if enabled else 'OFF'}</b>", parse_mode=ParseMode.HTML)
            return
        enabled = arg == "on"
        await set_target_chat_autosave_mode(chat_id, enabled, message.from_user.id)
        await message.reply(
            f"Target Chat Auto-save mode: <b>{'ON' if enabled else 'OFF'}</b>\n"
            f"{'ဒီ target chat ထဲမှာ save-only mode ON ဖြစ်ပါပြီ။' if enabled else 'ဒီ target chat ထဲမှာ save-only mode OFF ဖြစ်သွားပါပြီ။'}",
            parse_mode=ParseMode.HTML,
        )
        return
    user_id = message.from_user.id
    if arg == "status":
        enabled = await get_autosave_mode(user_id)
        await message.reply(f"Auto-save mode: <b>{'ON' if enabled else 'OFF'}</b>", parse_mode=ParseMode.HTML)
        return
    enabled = arg == "on"
    await set_autosave_mode(user_id, enabled)
    await message.reply(
        f"Auto-save mode: <b>{'ON' if enabled else 'OFF'}</b>\n"
        f"{'Save-only mode ဝင်ပါပြီ။' if enabled else 'Save-only mode ပိတ်လိုက်ပါပြီ။'}",
        parse_mode=ParseMode.HTML,
    )


@router.message(Command("save"))
async def save_handler(message: Message, command: CommandObject, bot: Bot) -> None:
    await remember_chat(message)
    if not await can_save(message):
        return
    target = message.reply_to_message or message
    media_type, _media = extract_media_handle(target)
    if not media_type:
        await message.reply("/save ကို media message ကို reply ပြီးသုံးပါ")
        return
    parsed = get_effective_parsed_message(target)
    if command.args:
        parsed.name = clean_value(command.args)
    if not parsed.name:
        await message.reply("name မတွေ့ပါ။\nအသုံးပြုပုံ: replied media ပေါ်မှာ /save Nahida")
        return
    try:
        meta = await get_media_meta(bot, target)
        doc, created = await upsert_item(meta=meta, parsed=parsed, saved_by=message.from_user.id)
    except Exception as exc:
        logger.exception("save failed")
        await message.reply(f"save မအောင်မြင်ပါ: {exc}")
        return
    try:
        await send_added_log(
            bot=bot,
            source_message=target,
            doc=doc,
            created=created,
            mode="manual-save",
            source_label=get_log_source_label(target),
            added_by_user=message.from_user,
        )
    except Exception:
        logger.exception("added log send failed")
    status = "Saved" if created else "Updated"
    await message.reply(
        f"{status}: <b>{html_escape(doc['name'])}</b>\n"
        f"Type: <b>{doc['media_type']}</b>\n"
        f"Cmd: <code>{html_escape(doc['command_name'])}</code>",
        parse_mode=ParseMode.HTML,
    )


@router.message(F.photo | F.video)
async def media_handler(message: Message, bot: Bot) -> None:
    await remember_chat(message)
    media_type, _media = extract_media_handle(message)
    if not media_type:
        return
    user_can_save = await can_save(message)
    user_id = message.from_user.id if message.from_user else None
    autosave_enabled = await get_autosave_mode(user_id)
    parsed = get_effective_parsed_message(message)
    supported_source = bool(is_allowed_forward_source(message) or get_inline_source_command(message) or is_character_catcher_style_message(message))

    # target chat save-only mode
    if is_default_target_chat(message):
        target_chat_autosave_enabled = await get_target_chat_autosave_mode(message.chat.id)
        if not target_chat_autosave_enabled:
            return
        if not supported_source:
            return
        if not parsed.name:
            await message.reply("name မတွေ့ပါ။ supported post ကို forward / send လုပ်ပါ။")
            return
        try:
            meta = await get_media_meta(bot, message)
            doc, created = await upsert_item(meta=meta, parsed=parsed, saved_by=user_id or 0)
            source_label = get_autosave_source_label(message)
            try:
                await send_added_log(
                    bot=bot,
                    source_message=message,
                    doc=doc,
                    created=created,
                    mode="auto-save",
                    source_label=str(source_label),
                    added_by_user=message.from_user,
                )
            except Exception:
                logger.exception("added log send failed")
            status = "Saved" if created else "Updated"
            await message.reply(
                f"{status}: <b>{html_escape(doc['name'])}</b>\n"
                f"ID: <b>{html_escape(doc.get('card_id') or '-')}</b>\n"
                f"Mode: <b>auto-save</b>\n"
                f"Source: <b>{html_escape(str(source_label))}</b>\n"
                f"Cmd: <code>{html_escape(doc['command_name'])}</code>",
                parse_mode=ParseMode.HTML,
            )
        except Exception as exc:
            logger.exception("target save-only failed")
            await message.reply(f"target auto-save error: {exc}")
        return

    # DM autosave mode
    if is_private_chat(message) and user_can_save and autosave_enabled:
        if not supported_source:
            if is_forwarded_message(message):
                await message.reply("ဒီ forwarded source ကို auto-save ခွင့်မပြုထားသေးပါဘူး။")
            return
        if not parsed.name:
            await message.reply("name မတွေ့ပါ။ supported post ကို forward / send လုပ်ပါ။")
            return
        try:
            meta = await get_media_meta(bot, message)
            doc, created = await upsert_item(meta=meta, parsed=parsed, saved_by=user_id or 0)
            source_label = get_autosave_source_label(message)
            try:
                await send_added_log(
                    bot=bot,
                    source_message=message,
                    doc=doc,
                    created=created,
                    mode="auto-save",
                    source_label=str(source_label),
                    added_by_user=message.from_user,
                )
            except Exception:
                logger.exception("added log send failed")
            status = "Saved" if created else "Updated"
            await message.reply(
                f"{status}: <b>{html_escape(doc['name'])}</b>\n"
                f"Mode: <b>auto-save</b>\n"
                f"Source: <b>{html_escape(str(source_label))}</b>\n"
                f"Cmd: <code>{html_escape(doc['command_name'])}</code>",
                parse_mode=ParseMode.HTML,
            )
        except Exception as exc:
            logger.exception("auto-save failed")
            await message.reply(f"auto-save error: {exc}")
        return


def normalize_webhook_path(path: str) -> str:
    return path if path.startswith("/") else f"/{path}"


async def on_startup(bot: Bot) -> None:
    await ensure_indexes()
    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Start the adding bot"),
            BotCommand(command="status", description="Show adding bot status"),
            BotCommand(command="stats", description="Show adding bot stats"),
        ]
    )
    me = await bot.get_me()
    logger.info("Bot started as @%s", me.username)
    logger.info("Configured inline source bots: %s", sorted(INLINE_SOURCE_BOTS) if INLINE_SOURCE_BOTS else "none")
    logger.info("Forward source username commands: %s", FORWARD_SOURCE_USERNAME_COMMAND_MAP or "none")
    logger.info("Forward source title commands: %s", FORWARD_SOURCE_TITLE_COMMAND_MAP or "none")
    logger.info("Added log channel: %s", ADDED_LOG_CHANNEL or "none")
    logger.info("Default target chat: %s", DEFAULT_TARGET_CHAT if DEFAULT_TARGET_CHAT is not None else "none")
    logger.info("Mode: %s", "WEBHOOK" if USE_WEBHOOK else "POLLING")


async def health_handler(_request: web.Request) -> web.Response:
    return web.json_response({"ok": True, "service": "adding-bot", "mode": "webhook" if USE_WEBHOOK else "polling"})


async def start_web_app(dp: Dispatcher, bot: Bot):
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/healthz", health_handler)

    if USE_WEBHOOK:
        webhook_path = normalize_webhook_path(WEBHOOK_PATH)
        SimpleRequestHandler(dispatcher=dp, bot=bot, secret_token=WEBHOOK_SECRET).register(app, path=webhook_path)
        setup_application(app, dp, bot=bot)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

    if USE_WEBHOOK:
        webhook_url = f"{PUBLIC_URL.rstrip('/')}{normalize_webhook_path(WEBHOOK_PATH)}"
        await bot.set_webhook(url=webhook_url, secret_token=WEBHOOK_SECRET, drop_pending_updates=False)
        logger.info("Webhook set to %s", webhook_url)

    return runner


async def main() -> None:
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)
    await on_startup(bot)
    runner = await start_web_app(dp, bot)
    try:
        if USE_WEBHOOK:
            await asyncio.Event().wait()
        else:
            await bot.delete_webhook(drop_pending_updates=False)
            await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        try:
            if USE_WEBHOOK:
                await bot.delete_webhook(drop_pending_updates=False)
        except Exception:
            pass
        try:
            await bot.session.close()
        except Exception:
            pass
        try:
            await runner.cleanup()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped")
