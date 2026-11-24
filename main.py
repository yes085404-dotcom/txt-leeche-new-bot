import os
import re
import json
import time
import asyncio
import logging
import threading
import subprocess
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import aiohttp
from pyrogram import Client, filters, idle
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import (
    CallbackQuery,
    ForceReply,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("CourseUploaderBot")

# ==================== RENDER ENV VARIABLES ====================
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "").strip()
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
BOT_PASSWORD = os.environ.get("BOT_PASSWORD", "").strip()
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))
DEFAULT_CREDIT = os.environ.get("DEFAULT_CREDIT", "Extract by Mr.Sanskari").strip()
PORT = int(os.environ.get("PORT", "8000"))

if not API_ID or not API_HASH or not BOT_TOKEN:
    raise RuntimeError("Missing required env vars: API_ID, API_HASH, BOT_TOKEN")
if not BOT_PASSWORD:
    raise RuntimeError("Missing required env var: BOT_PASSWORD")

BASE_DIR = Path(__file__).resolve().parent
DOWNLOAD_DIR = BASE_DIR / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)
DATA_FILE = BASE_DIR / "bot_data.json"


# ==================== KEEP-ALIVE SERVER FOR RENDER ====================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, format, *args):
        return


def start_health_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()


# ==================== PERSISTENT STORAGE ====================
def load_db() -> dict:
    if DATA_FILE.exists():
        try:
            return json.loads(DATA_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "unlocked_users": [],
        "premium_users": {},
        "user_settings": {},
    }


DB = load_db()
DB_LOCK = threading.Lock()


def save_db():
    with DB_LOCK:
        DATA_FILE.write_text(json.dumps(DB, indent=2), encoding="utf-8")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def get_premium_expiry(user_id: int) -> datetime | None:
    rec = DB.get("premium_users", {}).get(str(user_id))
    if not rec:
        return None
    try:
        exp = datetime.fromisoformat(rec["expires_at"])
        if exp > now_utc():
            return exp
    except Exception:
        pass
    return None


def grant_premium(user_id: int, days: int) -> datetime:
    current_exp = get_premium_expiry(user_id)
    base = current_exp if current_exp else now_utc()
    new_exp = base + timedelta(days=days)
    DB.setdefault("premium_users", {})[str(user_id)] = {
        "expires_at": new_exp.isoformat(),
        "days": days,
    }
    save_db()
    return new_exp


def is_owner(user_id: int) -> bool:
    return OWNER_ID != 0 and user_id == OWNER_ID


def is_authorized(user_id: int) -> bool:
    if is_owner(user_id):
        return True
    if get_premium_expiry(user_id) is not None:
        return True
    return user_id in DB.get("unlocked_users", [])


def mark_unlocked(user_id: int):
    unlocked = DB.setdefault("unlocked_users", [])
    if user_id not in unlocked:
        unlocked.append(user_id)
        save_db()


def lock_user(user_id: int):
    unlocked = DB.setdefault("unlocked_users", [])
    if user_id in unlocked:
        unlocked.remove(user_id)
        save_db()


def get_user_cfg(user_id: int) -> dict:
    key = str(user_id)
    cfgs = DB.setdefault("user_settings", {})
    if key not in cfgs:
        cfgs[key] = {
            "channel": None,
            "credit": DEFAULT_CREDIT,
            "token": "",
            "topic_cache": {},
        }
        save_db()
    cfg = cfgs[key]
    cfg.setdefault("channel", None)
    cfg.setdefault("credit", DEFAULT_CREDIT)
    cfg.setdefault("token", "")
    cfg.setdefault("topic_cache", {})
    return cfg


# ==================== RUNTIME WORKFLOW STATE ====================
PENDING_FLOW = {}
PENDING_BATCH = {}
FAILED_BATCH = {}
ACTIVE_JOBS = {}


# ==================== FORMATTING & PARSING HELPERS ====================
def fmt_size(num_bytes: float | int) -> str:
    val = float(max(0, num_bytes))
    for unit in ["B", "KB", "MB", "GB"]:
        if val < 1024.0:
            return f"{val:.1f} {unit}"
        val /= 1024.0
    return f"{val:.2f} TB"


def fmt_speed(bytes_per_sec: float) -> str:
    if bytes_per_sec <= 0:
        return "0.0 KB/s"
    return f"{fmt_size(bytes_per_sec)}/s"


def fmt_eta(seconds: float | None) -> str:
    if seconds is None or seconds < 0 or seconds == float("inf"):
        return "--m --s"
    sec = int(seconds)
    mins, rem = divmod(sec, 60)
    hrs, mins = divmod(mins, 60)
    if hrs > 0:
        return f"{hrs}h {mins:02d}m {rem:02d}s"
    return f"{mins:02d}m {rem:02d}s"


def make_bar(pct: float, width: int = 20) -> str:
    clamped = max(0.0, min(100.0, pct))
    filled = int(round((clamped / 100.0) * width))
    return "█" * filled + "░" * (width - filled)


def extract_leading_groups(prefix: str) -> tuple[list[str], str]:
    groups = []
    s = prefix.strip()
    while s and s[0] in "([":
        open_ch = s[0]
        close_ch = ")" if open_ch == "(" else "]"
        depth = 0
        end_idx = -1
        for i, ch in enumerate(s):
            if ch == open_ch:
                depth += 1
            elif ch == close_ch:
                depth -= 1
                if depth == 0:
                    end_idx = i
                    break
        if end_idx == -1:
            break
        grp = s[1:end_idx].strip()
        if grp:
            groups.append(grp)
        s = s[end_idx + 1 :].strip(" :-|")
    return groups, s.strip()


def parse_course_txt(raw_text: str) -> tuple[list[tuple[str, str, str]], int, int]:
    parsed = []
    seen_urls = set()
    encrypted_count = 0
    duplicate_count = 0
    current_topic = "General"
    known_subjects = {
        "physics", "chemistry", "biology", "math", "maths",
        "mathematics", "english", "hindi", "urdu", "sanskrit",
    }

    for raw_line in raw_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or line.startswith("==="):
            continue

        if "auratx://" in line.lower():
            encrypted_count += 1
            continue

        if ":" not in line and "http" not in line.lower() and line.lower() in known_subjects:
            current_topic = line
            continue

        if line.lower().startswith("topic:") and "http" not in line.lower():
            t = line.split(":", 1)[1].strip()
            if t:
                current_topic = t
            continue

        link_match = re.search(r"(https?://\S+)", line, re.IGNORECASE)
        if not link_match:
            continue

        url = link_match.group(1).strip().rstrip(",;")
        if url in seen_urls:
            duplicate_count += 1
        else:
            seen_urls.add(url)

        prefix = line[: link_match.start()].strip().rstrip(":-|").strip()

        topic = current_topic
        title = prefix or "Untitled Item"

        if "|" in prefix:
            parts = [p.strip() for p in prefix.split("|", 1)]
            if parts[0]:
                topic = parts[0]
            if len(parts) > 1 and parts[1]:
                title = parts[1]
        else:
            bracket_match = re.search(r"\[([^\]]+)\]", prefix)
            if bracket_match:
                b_val = bracket_match.group(1).strip()
                if "→" in b_val:
                    sub_parts = [p.strip() for p in b_val.split("→") if p.strip()]
                    topic = " - ".join(sub_parts[-2:]) if len(sub_parts) >= 2 else sub_parts[0]
                elif b_val:
                    topic = b_val

                cleaned = re.sub(r"\[[^\]]+\]", "", prefix)
                cleaned = re.sub(r"^\s*Crash Course\s*-\s*\d{4}\s*", "", cleaned, flags=re.IGNORECASE)
                cleaned = cleaned.strip(" :-|")
                if cleaned:
                    title = cleaned
            else:
                groups, rem_title = extract_leading_groups(prefix)
                if groups:
                    if len(groups) >= 2:
                        topic = f"{groups[1]} - {groups[-1]}" if groups[1] != groups[-1] else groups[1]
                    else:
                        topic = groups[0]
                    if rem_title:
                        title = rem_title

        parsed.append((topic, title, url))

    return parsed, encrypted_count, duplicate_count


def classify_media(url: str) -> str:
    low = url.lower()
    if "pdf" in low and "/fetch_video" not in low:
        return "doc"
    if re.search(r"\.(pdf|doc|docx|ppt|pptx|xls|xlsx|zip|rar|txt)(\?|$)", low):
        return "doc"
    if re.search(r"\.(jpg|jpeg|png|webp|gif)(\?|$)", low):
        return "photo"
    return "video"


def apply_fresh_token_to_url(url: str, token: str) -> str:
    if not token:
        return url
    parsed = urlparse(url)
    qs = parse_qs(parsed.query, keep_blank_values=True)
    if "token" in qs:
        qs["token"] = [token]
        new_query = urlencode(qs, doseq=True)
        return urlunparse(parsed._replace(query=new_query))
    return url


async def resolve_url(url: str, token: str) -> tuple[str, dict]:
    effective_url = apply_fresh_token_to_url(url, token)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36"
        ),
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
        headers["token"] = token

    parsed = urlparse(effective_url)
    qs = parse_qs(parsed.query)
    if "api_base" in qs and qs["api_base"]:
        headers["Referer"] = f"https://{qs['api_base'][0]}/"

    if "/fetch_video" in parsed.path:
        timeout = aiohttp.ClientTimeout(total=30)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(effective_url, headers=headers, allow_redirects=True) as resp:
                    if resp.status == 200:
                        ctype = (resp.headers.get("Content-Type") or "").lower()
                        if "application/json" in ctype:
                            data = await resp.json(content_type=None)
                            for key in ("url", "link", "video_url", "download_url", "stream_url"):
                                val = data.get(key) if isinstance(data, dict) else None
                                if isinstance(val, str) and val.startswith("http"):
                                    return val, headers
                        return str(resp.url), headers
        except Exception:
            pass

    return effective_url, headers


def build_live_status_text(job: dict) -> str:
    total = job["total"]
    done = job["done"]
    failed = job["failed"]
    rem = max(0, total - done)

    dl = job["dl"]
    ul = job["ul"]

    dl_bar = make_bar(dl["pct"])
    ul_bar = make_bar(ul["pct"])

    dl_size_line = (
        f"{fmt_size(dl['done_bytes'])} / {fmt_size(dl['total_bytes'])}"
        if dl["total_bytes"] > 0
        else f"{fmt_size(dl['done_bytes'])}"
    )
    ul_size_line = (
        f"{fmt_size(ul['done_bytes'])} / {fmt_size(ul['total_bytes'])}"
        if ul["total_bytes"] > 0
        else f"{fmt_size(ul['done_bytes'])}"
    )

    return (
        f"📊 **LIVE BATCH STATUS** | Mode: `{job['mode_label']}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 **Batch Progress** : `{done} / {total} Done` (`{rem} Left`)\n"
        f"✅ Uploaded: `{done - failed}` | ❌ Failed: `{failed}` | 🔁 Try: `{job['attempt']}/3`\n"
        f"🏷️ **Credit** : `{job['credit']}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎬 **Current Item** `[{job['idx']}/{total}]`:\n"
        f"**Title** : `{job['title'][:90]}`\n"
        f"🗂️ **Topic** : `{job['topic'][:50]}`\n\n"
        f"⬇️ **DOWNLOAD STATUS** ({dl['note']})\n"
        f"[{dl_bar}] `{dl['pct']:.1f}%`\n"
        f"📦 **Size**  : `{dl_size_line}`\n"
        f"⚡ **Speed** : `{fmt_speed(dl['speed'])}`\n"
        f"⏳ **ETA**   : `{fmt_eta(dl['eta'])}`\n\n"
        f"⬆️ **UPLOAD STATUS** ({ul['note']})\n"
        f"[{ul_bar}] `{ul['pct']:.1f}%`\n"
        f"📦 **Size**  : `{ul_size_line}`\n"
        f"⚡ **Speed** : `{fmt_speed(ul['speed'])}`\n"
        f"⏳ **ETA**   : `{fmt_eta(ul['eta'])}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🛑 Stop: `/stop`   |   📈 Status: `/status`"
    )


def main_menu_keyboard(user_id: int) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("📄 Upload TXT (/txt)", callback_data="act_txt"),
            InlineKeyboardButton("📢 Set Channel", callback_data="act_setchannel"),
        ],
        [
            InlineKeyboardButton("🏷️ Set Credit", callback_data="act_setcredit"),
            InlineKeyboardButton("📊 Live Status", callback_data="act_status"),
        ],
        [
            InlineKeyboardButton("🔁 Retry Failed", callback_data="act_retry"),
            InlineKeyboardButton("🛑 Stop Batch", callback_data="act_stop"),
        ],
        [
            InlineKeyboardButton("🔒 Lock Session", callback_data="act_lock"),
        ],
    ]
    if is_owner(user_id):
        rows.append([InlineKeyboardButton("👑 Premium Users Plan", callback_data="act_premium")])
    return InlineKeyboardMarkup(rows)


def mode_selection_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📢 /channel", callback_data="mode_channel"),
                InlineKeyboardButton("🗂️ /grouptopictransfer", callback_data="mode_topic"),
            ]
        ]
    )


def premium_days_keyboard(target_uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("1 Day", callback_data=f"prem_{target_uid}_1"),
                InlineKeyboardButton("2 Days", callback_data=f"prem_{target_uid}_2"),
                InlineKeyboardButton("10 Days", callback_data=f"prem_{target_uid}_10"),
            ],
            [
                InlineKeyboardButton("1 Month (30d)", callback_data=f"prem_{target_uid}_30"),
                InlineKeyboardButton("Custom Days", callback_data=f"prem_{target_uid}_custom"),
            ],
        ]
    )


# ==================== DOWNLOADERS ====================
async def download_http_file(url: str, out_path: Path, headers: dict, job: dict, loop: asyncio.AbstractEventLoop):
    dl = job["dl"]
    dl["note"] = "Downloading"
    dl["pct"] = 0.0
    dl["done_bytes"] = 0
    dl["total_bytes"] = 0
    dl["speed"] = 0.0
    dl["eta"] = None

    last_sample_t = time.monotonic()
    last_sample_bytes = 0
    last_ui_t = 0.0

    timeout = aiohttp.ClientTimeout(total=3600, sock_connect=30, sock_read=120)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url, headers=headers) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("Content-Length") or 0)
            dl["total_bytes"] = total

            with open(out_path, "wb") as f:
                async for chunk in resp.content.iter_chunked(256 * 1024):
                    if job.get("cancel"):
                        raise RuntimeError("Cancelled by user")
                    if not chunk:
                        continue
                    f.write(chunk)
                    dl["done_bytes"] += len(chunk)

                    now = time.monotonic()
                    dt = now - last_sample_t
                    if dt >= 0.6:
                        dbytes = dl["done_bytes"] - last_sample_bytes
                        dl["speed"] = dbytes / dt
                        if total > 0 and dl["speed"] > 0:
                            dl["eta"] = max(0.0, (total - dl["done_bytes"]) / dl["speed"])
                            dl["pct"] = min(100.0, (dl["done_bytes"] / total) * 100.0)
                        last_sample_t = now
                        last_sample_bytes = dl["done_bytes"]

                    if now - last_ui_t >= 2.2:
                        last_ui_t = now
                        await push_live_status(job, loop)

    if dl["total_bytes"] <= 0:
        dl["total_bytes"] = out_path.stat().st_size if out_path.exists() else 0
    dl["done_bytes"] = dl["total_bytes"]
    dl["pct"] = 100.0
    dl["eta"] = 0.0
    dl["note"] = "Complete"
    await push_live_status(job, loop)


def run_yt_dlp_with_progress(url: str, base_out: Path, headers: dict, job: dict, loop: asyncio.AbstractEventLoop) -> Path:
    cmd = [
        "yt-dlp",
        "--no-warnings",
        "--newline",
        "--retries",
        "5",
        "--fragment-retries",
        "5",
        "--concurrent-fragments",
        "4",
        "-f",
        "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
        "--merge-output-format",
        "mp4",
        "-o",
        f"{base_out}.%(ext)s",
    ]
    for k, v in (headers or {}).items():
        if v:
            cmd.extend(["--add-header", f"{k}: {v}"])
    cmd.append(url)

    dl = job["dl"]
    dl["note"] = "Stream / Video"
    dl["pct"] = 0.0
    dl["done_bytes"] = 0
    dl["total_bytes"] = 0
    dl["speed"] = 0.0
    dl["eta"] = None

    last_ui_t = [0.0]

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    pct_re = re.compile(r"\[download\]\s+(\d+(?:\.\d+)?)%")
    size_re = re.compile(r"of\s+~?\s*([\d.]+\s*[KMGTP]?i?B)", re.IGNORECASE)
    speed_re = re.compile(r"at\s+([\d.]+\s*[KMGTP]?i?B/s|Unknown)", re.IGNORECASE)
    eta_re = re.compile(r"ETA\s+(\d{2}:\d{2}(?::\d{2})?)", re.IGNORECASE)

    def parse_human_bytes(s: str) -> float:
        m = re.match(r"([\d.]+)\s*([KMGTP]?i?B)", s.strip(), re.IGNORECASE)
        if not m:
            return 0.0
        num = float(m.group(1))
        unit = m.group(2).upper()
        mult = {
            "B": 1,
            "KB": 1000, "KIB": 1024,
            "MB": 1000**2, "MIB": 1024**2,
            "GB": 1000**3, "GIB": 1024**3,
            "TB": 1000**4, "TIB": 1024**4,
        }
        return num * mult.get(unit, 1)

    def parse_eta_sec(s: str) -> float:
        parts = [int(x) for x in s.split(":")]
        if len(parts) == 2:
            return parts[0] * 60 + parts[1]
        if len(parts) == 3:
            return parts[0] * 3600 + parts[1] * 60 + parts[2]
        return 0.0

    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            if job.get("cancel"):
                proc.terminate()
                raise RuntimeError("Cancelled by user")

            m_pct = pct_re.search(line)
            if m_pct:
                pct = float(m_pct.group(1))
                dl["pct"] = min(100.0, pct)

                m_sz = size_re.search(line)
                if m_sz:
                    tot = parse_human_bytes(m_sz.group(1))
                    dl["total_bytes"] = tot
                    dl["done_bytes"] = tot * (dl["pct"] / 100.0)

                m_sp = speed_re.search(line)
                if m_sp and "unknown" not in m_sp.group(1).lower():
                    raw_sp = m_sp.group(1).replace("/s", "")
                    dl["speed"] = parse_human_bytes(raw_sp)

                m_eta = eta_re.search(line)
                if m_eta:
                    dl["eta"] = parse_eta_sec(m_eta.group(1))

                now = time.monotonic()
                if now - last_ui_t[0] >= 2.2:
                    last_ui_t[0] = now
                    fut = asyncio.run_coroutine_threadsafe(push_live_status(job, loop), loop)
                    try:
                        fut.result(timeout=3)
                    except Exception:
                        pass

        rc = proc.wait()
        if rc != 0:
            raise RuntimeError(f"yt-dlp exited with code {rc}")
    finally:
        if proc.poll() is None:
            proc.kill()

    for candidate in DOWNLOAD_DIR.glob(f"{base_out.name}.*"):
        if candidate.suffix.lower() not in {".part", ".ytdl", ".temp"}:
            fsize = candidate.stat().st_size
            dl["total_bytes"] = fsize
            dl["done_bytes"] = fsize
            dl["pct"] = 100.0
            dl["eta"] = 0.0
            dl["note"] = "Complete"
            return candidate

    raise FileNotFoundError("Downloaded media file not found")


# ==================== UPLOAD & LIVE STATUS ====================
async def push_live_status(job: dict, loop: asyncio.AbstractEventLoop):
    status_msg: Message | None = job.get("status_msg")
    if not status_msg:
        return
    text = build_live_status_text(job)
    if text == job.get("last_rendered"):
        return
    job["last_rendered"] = text
    try:
        await status_msg.edit_text(text)
    except FloodWait as fw:
        await asyncio.sleep(fw.value + 1)
    except RPCError:
        pass


async def telegram_upload_cb(current: int, total: int, job: dict, loop: asyncio.AbstractEventLoop, tracker: dict):
    if job.get("cancel"):
        raise RuntimeError("Cancelled by user")
    ul = job["ul"]
    now = time.monotonic()
    dt = now - tracker["last_t"]

    ul["total_bytes"] = total or ul["total_bytes"]
    ul["done_bytes"] = current
    if total:
        ul["pct"] = min(100.0, (current / total) * 100.0)

    if dt >= 0.6:
        dbytes = current - tracker["last_bytes"]
        ul["speed"] = dbytes / dt
        if total and ul["speed"] > 0:
            ul["eta"] = max(0.0, (total - current) / ul["speed"])
        tracker["last_t"] = now
        tracker["last_bytes"] = current

    if now - tracker["last_ui"] >= 2.2:
        tracker["last_ui"] = now
        await push_live_status(job, loop)


async def ensure_forum_topic(client: Client, chat_id: int, topic_name: str, cfg: dict) -> int | None:
    clean_topic = (topic_name or "General").strip()[:120] or "General"
    cache = cfg.setdefault("topic_cache", {})
    cache_key = f"{chat_id}:{clean_topic}"
    if cache_key in cache:
        return int(cache[cache_key])
    try:
        topic = await client.create_forum_topic(chat_id=chat_id, title=clean_topic)
        thread_id = topic.id
        cache[cache_key] = thread_id
        save_db()
        return thread_id
    except FloodWait as fw:
        await asyncio.sleep(fw.value + 1)
        return await ensure_forum_topic(client, chat_id, topic_name, cfg)
    except Exception as e:
        logger.warning("create_forum_topic failed for %s: %s", clean_topic, e)
        return None


async def send_with_flood_guard(coro_fn):
    while True:
        try:
            return await coro_fn()
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 2)


async def run_batch_job(client: Client, user_id: int, mode: str, custom_items: list | None = None):
    cfg = get_user_cfg(user_id)
    batch = PENDING_BATCH.get(user_id)
    items = custom_items if custom_items is not None else (batch["items"] if batch else [])
    if not items:
        return

    channel_id = cfg["channel"]
    if not channel_id:
        await client.send_message(user_id, "❌ Target channel/group set nahi hai. Pehle `/setchannel` karein.")
        return

    credit = cfg["credit"] or DEFAULT_CREDIT
    token = cfg["token"]
    mode_label = "Group Topics" if mode == "topic" else "Channel"
    loop = asyncio.get_running_loop()
    FAILED_BATCH[user_id] = {"items": [], "mode": mode}

    status_msg = await client.send_message(
        user_id,
        f"⏳ Batch start ho raha hai (`{len(items)}` items) | Mode: `{mode_label}`...",
    )

    job = {
        "cancel": False,
        "mode": mode,
        "mode_label": mode_label,
        "credit": credit,
        "total": len(items),
        "done": 0,
        "failed": 0,
        "idx": 1,
        "attempt": 1,
        "title": items[0][1],
        "topic": items[0][0],
        "status_msg": status_msg,
        "last_rendered": "",
        "dl": {"pct": 0.0, "done_bytes": 0, "total_bytes": 0, "speed": 0.0, "eta": None, "note": "Idle"},
        "ul": {"pct": 0.0, "done_bytes": 0, "total_bytes": 0, "speed": 0.0, "eta": None, "note": "Waiting"},
    }
    ACTIVE_JOBS[user_id] = job

    try:
        for idx, (topic, title, url) in enumerate(items, start=1):
            if job.get("cancel"):
                break

            job["idx"] = idx
            job["topic"] = topic
            job["title"] = title
            safe_stem = re.sub(r'[\\/*?:"<>|]+', "_", title)[:75] or f"item_{idx}"

            thread_id = None
            if mode == "topic":
                thread_id = await ensure_forum_topic(client, channel_id, topic, cfg)

            caption = f"**{title}**\n🗂️ Topic: `{topic}`\n\n{credit}"
            uploaded_ok = False
            last_err = None

            for attempt in range(1, 4):
                if job.get("cancel"):
                    break
                job["attempt"] = attempt
                job["dl"] = {"pct": 0.0, "done_bytes": 0, "total_bytes": 0, "speed": 0.0, "eta": None, "note": "Starting"}
                job["ul"] = {"pct": 0.0, "done_bytes": 0, "total_bytes": 0, "speed": 0.0, "eta": None, "note": "Waiting"}
                await push_live_status(job, loop)

                downloaded_path: Path | None = None
                try:
                    resolved_url, req_headers = await resolve_url(url, token)
                    media_kind = classify_media(resolved_url)

                    if media_kind in ("doc", "photo"):
                        ext = ".pdf" if "pdf" in resolved_url.lower() else (".jpg" if media_kind == "photo" else ".bin")
                        m_ext = re.search(r"\.([a-zA-Z0-9]{2,5})(?:\?|$)", resolved_url)
                        if m_ext:
                            ext = "." + m_ext.group(1).lower()
                        out_file = DOWNLOAD_DIR / f"{user_id}_{idx}_{safe_stem}{ext}"
                        await download_http_file(resolved_url, out_file, req_headers, job, loop)
                        downloaded_path = out_file
                    else:
                        base_out = DOWNLOAD_DIR / f"{user_id}_{idx}_{safe_stem}"
                        downloaded_path = await asyncio.to_thread(
                            run_yt_dlp_with_progress, resolved_url, base_out, req_headers, job, loop
                        )
                        fut = asyncio.run_coroutine_threadsafe(push_live_status(job, loop), loop)
                        fut.result(timeout=5)

                    fsize = downloaded_path.stat().st_size if downloaded_path and downloaded_path.exists() else 0
                    job["ul"] = {
                        "pct": 0.0,
                        "done_bytes": 0,
                        "total_bytes": fsize,
                        "speed": 0.0,
                        "eta": None,
                        "note": "Uploading",
                    }
                    await push_live_status(job, loop)

                    tracker = {
                        "last_t": time.monotonic(),
                        "last_bytes": 0,
                        "last_ui": 0.0,
                    }

                    send_kwargs = {
                        "chat_id": channel_id,
                        "caption": caption,
                        "progress": telegram_upload_cb,
                        "progress_args": (job, loop, tracker),
                    }
                    if thread_id is not None:
                        send_kwargs["message_thread_id"] = thread_id

                    if media_kind == "photo":
                        await send_with_flood_guard(
                            lambda: client.send_photo(photo=str(downloaded_path), **send_kwargs)
                        )
                    elif media_kind == "doc":
                        await send_with_flood_guard(
                            lambda: client.send_document(document=str(downloaded_path), **send_kwargs)
                        )
                    else:
                        await send_with_flood_guard(
                            lambda: client.send_video(
                                video=str(downloaded_path),
                                supports_streaming=True,
                                **send_kwargs,
                            )
                        )

                    job["ul"]["pct"] = 100.0
                    job["ul"]["done_bytes"] = fsize
                    job["ul"]["eta"] = 0.0
                    job["ul"]["note"] = "Complete"
                    uploaded_ok = True
                    break

                except Exception as exc:
                    last_err = exc
                    logger.warning("Item %s attempt %d/3 failed: %s", title, attempt, exc)
                    if attempt < 3 and not job.get("cancel"):
                        await asyncio.sleep(2 * attempt)
                finally:
                    if downloaded_path and downloaded_path.exists():
                        downloaded_path.unlink(missing_ok=True)
                    for p in DOWNLOAD_DIR.glob(f"{user_id}_{idx}_{safe_stem}*"):
                        p.unlink(missing_ok=True)

            if uploaded_ok:
                job["done"] += 1
                await push_live_status(job, loop)
            else:
                job["failed"] += 1
                job["done"] += 1
                job["ul"]["note"] = "Failed"
                FAILED_BATCH[user_id]["items"].append((topic, title, url))
                try:
                    fail_kwargs = {
                        "chat_id": channel_id,
                        "text": (
                            f"❌ **Failed (after 3 tries):** `{title}`\n"
                            f"🗂️ Topic: `{topic}`\n"
                            f"🔗 Link: {url}\n"
                            f"⚠️ Reason: `{str(last_err)[:180]}`\n\n{credit}"
                        ),
                        "disable_web_page_preview": True,
                    }
                    if thread_id is not None:
                        fail_kwargs["message_thread_id"] = thread_id
                    await send_with_flood_guard(lambda: client.send_message(**fail_kwargs))
                except Exception:
                    pass
                await push_live_status(job, loop)

    finally:
        was_cancelled = bool(job.get("cancel"))
        ACTIVE_JOBS.pop(user_id, None)
        failed_cnt = len(FAILED_BATCH.get(user_id, {}).get("items", []))
        retry_hint = f"\n🔁 Failed items dubara chalane ke liye `/retryfailed` bhejein (`{failed_cnt}` items)." if failed_cnt > 0 else ""
        summary = (
            f"{'🛑 **BATCH STOPPED**' if was_cancelled else '✅ **BATCH COMPLETED**'}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📦 Total Processed: `{job['done']} / {job['total']}`\n"
            f"✅ Success: `{job['done'] - job['failed']}`\n"
            f"❌ Failed: `{job['failed']}`\n"
            f"🏷️ Credit: `{credit}`{retry_hint}"
        )
        try:
            await status_msg.edit_text(summary)
        except Exception:
            await client.send_message(user_id, summary)


# ==================== BOT CLIENT & HANDLERS ====================
app = Client(
    "course_txt_uploader_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True,
)


async def guard_auth(client: Client, message: Message) -> bool:
    if not message.from_user:
        return False
    uid = message.from_user.id
    if is_authorized(uid):
        return True
    PENDING_FLOW[uid] = {"step": "password"}
    await message.reply_text(
        "🔐 **Bot Locked**\n\nIs bot ko use karne ke liye **Password** enter karein:",
        reply_markup=ForceReply(selective=True, placeholder="Enter password..."),
    )
    return False


async def send_home(client: Client, user_id: int, chat_id: int):
    cfg = get_user_cfg(user_id)
    prem_exp = get_premium_expiry(user_id)
    access_label = "Owner" if is_owner(user_id) else (f"Premium (till {prem_exp.strftime('%Y-%m-%d')})" if prem_exp else "Password Unlocked")
    await client.send_message(
        chat_id,
        f"🚀 **Course TXT Uploader Control Panel**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🛡️ Access: `{access_label}`\n"
        f"📢 Target ID: `{cfg['channel'] or 'Not Set'}`\n"
        f"🏷️ Credit: `{cfg['credit']}`\n"
        f"🔑 Token: `{'Set' if cfg['token'] else 'Not Set'}`\n\n"
        f"Commands: `/txt`, `/range 1 50`, `/retryfailed`, `/channel`, `/grouptopictransfer`",
        reply_markup=main_menu_keyboard(user_id),
    )


@app.on_message(filters.command("start") & filters.private)
async def cmd_start(client: Client, message: Message):
    if not await guard_auth(client, message):
        return
    await send_home(client, message.from_user.id, message.chat.id)


@app.on_message(filters.command("lock") & filters.private)
async def cmd_lock(client: Client, message: Message):
    uid = message.from_user.id
    if not is_owner(uid) and get_premium_expiry(uid) is None:
        lock_user(uid)
    PENDING_FLOW[uid] = {"step": "password"}
    await message.reply_text("🔒 Session lock ho gaya. Password daal kar unlock karein.")


@app.on_message(filters.command("setchannel") & filters.private)
async def cmd_setchannel(client: Client, message: Message):
    if not await guard_auth(client, message):
        return
    uid = message.from_user.id
    if len(message.command) >= 2:
        try:
            ch_id = int(message.command[1].strip())
            get_user_cfg(uid)["channel"] = ch_id
            save_db()
            await message.reply_text(f"✅ Target Channel/Group set: `{ch_id}`", reply_markup=main_menu_keyboard(uid))
            return
        except ValueError:
            await message.reply_text("❌ Valid numeric ID dein (jaise `-1001234567890`).")
            return

    PENDING_FLOW[uid] = {"step": "channel"}
    await message.reply_text(
        "📢 Target **Channel ya Group ID** bhejein (jaise `-1001234567890`).\n"
        "*(Dhyan rahe bot wahan Admin ho)*",
        reply_markup=ForceReply(selective=True, placeholder="-100..."),
    )


@app.on_message(filters.command("setcredit") & filters.private)
async def cmd_setcredit(client: Client, message: Message):
    if not await guard_auth(client, message):
        return
    uid = message.from_user.id
    parts = message.text.split(None, 1)
    if len(parts) >= 2 and parts[1].strip():
        get_user_cfg(uid)["credit"] = parts[1].strip()
        save_db()
        await message.reply_text(f"✅ Credit set: `{get_user_cfg(uid)['credit']}`", reply_markup=main_menu_keyboard(uid))
        return

    PENDING_FLOW[uid] = {"step": "credit"}
    await message.reply_text(
        "🏷️ Credit text bhejein (jaise `Extract by Mr.Sanskari`):",
        reply_markup=ForceReply(selective=True, placeholder="Extract by ..."),
    )


@app.on_message(filters.command("txt") & filters.private)
async def cmd_txt(client: Client, message: Message):
    if not await guard_auth(client, message):
        return
    uid = message.from_user.id
    PENDING_FLOW[uid] = {"step": "txt"}
    await message.reply_text("📄 Ab apni course **`.txt` file** upload karein:")


@app.on_message(filters.command("range") & filters.private)
async def cmd_range(client: Client, message: Message):
    if not await guard_auth(client, message):
        return
    uid = message.from_user.id
    batch = PENDING_BATCH.get(uid)
    if not batch or not batch.get("all_items"):
        await message.reply_text("❌ Pehle `.txt` file upload karein.")
        return
    if len(message.command) != 3:
        await message.reply_text("ℹ️ Use: `/range 1 50` (1 se 50 tak items select karne ke liye)")
        return
    try:
        start_i = max(1, int(message.command[1]))
        end_i = min(len(batch["all_items"]), int(message.command[2]))
        if start_i > end_i:
            raise ValueError()
        batch["items"] = batch["all_items"][start_i - 1 : end_i]
        await message.reply_text(
            f"✅ Range set: `{start_i}` se `{end_i}` (`{len(batch['items'])}` items).\n"
            f"Ab `/channel` ya `/grouptopictransfer` dabayein:",
            reply_markup=mode_selection_keyboard(),
        )
    except ValueError:
        await message.reply_text(f"❌ Valid range dein `1` se `{len(batch['all_items'])}` ke beech.")


@app.on_message(filters.command("retryfailed") & filters.private)
async def cmd_retry_failed(client: Client, message: Message):
    if not await guard_auth(client, message):
        return
    uid = message.from_user.id
    if uid in ACTIVE_JOBS:
        await message.reply_text("⚠️ Ek batch pehle se chal raha hai.")
        return
    rec = FAILED_BATCH.get(uid)
    if not rec or not rec.get("items"):
        await message.reply_text("✅ Koi failed item pending nahi hai.")
        return
    items_to_retry = list(rec["items"])
    asyncio.create_task(run_batch_job(client, uid, rec.get("mode", "channel"), custom_items=items_to_retry))


@app.on_message(filters.command("status") & filters.private)
async def cmd_status(client: Client, message: Message):
    if not await guard_auth(client, message):
        return
    uid = message.from_user.id
    job = ACTIVE_JOBS.get(uid)
    if not job:
        await message.reply_text("ℹ️ Abhi koi active batch nahi chal raha hai.")
        return
    await message.reply_text(build_live_status_text(job))


@app.on_message(filters.command("stop") & filters.private)
async def cmd_stop(client: Client, message: Message):
    if not await guard_auth(client, message):
        return
    uid = message.from_user.id
    job = ACTIVE_JOBS.get(uid)
    if not job:
        await message.reply_text("ℹ️ Chalane ke liye koi active batch nahi hai.")
        return
    job["cancel"] = True
    await message.reply_text("🛑 Stop signal bhej diya gaya hai. Current step ke baad ruk jayega.")


@app.on_message(filters.command(["channel", "grouptopictransfer"]) & filters.private)
async def cmd_transfer_mode(client: Client, message: Message):
    if not await guard_auth(client, message):
        return
    uid = message.from_user.id
    if uid not in PENDING_BATCH or not PENDING_BATCH[uid]["items"]:
        await message.reply_text("❌ Pehle `/txt` se `.txt` file aur token set karein.")
        return
    mode = "topic" if message.command[0].lower() == "grouptopictransfer" else "channel"
    if uid in ACTIVE_JOBS:
        await message.reply_text("⚠️ Ek batch pehle se chal raha hai. Pehle `/stop` karein.")
        return
    asyncio.create_task(run_batch_job(client, uid, mode))


@app.on_message(filters.document & filters.private)
async def on_document(client: Client, message: Message):
    if not await guard_auth(client, message):
        return
    uid = message.from_user.id
    doc = message.document
    if not doc.file_name or not doc.file_name.lower().endswith(".txt"):
        return

    tmp_path = DOWNLOAD_DIR / f"upload_{uid}_{doc.file_name}"
    await message.download(file_name=str(tmp_path))
    raw_text = tmp_path.read_text(encoding="utf-8", errors="ignore")
    tmp_path.unlink(missing_ok=True)

    items, enc_skipped, dup_count = parse_course_txt(raw_text)
    if not items:
        if enc_skipped > 0:
            await message.reply_text(
                f"❌ Is file me `{enc_skipped}` links `AURATX://:` encrypted format me hain.\n"
                f"Bina decryption key ke in links ko nikala nahi ja sakta. Kripya decrypted `https://` TXT file bhejein."
            )
        else:
            await message.reply_text("❌ Is `.txt` file me valid `https://` links nahi mile.")
        return

    PENDING_BATCH[uid] = {
        "all_items": list(items),
        "items": list(items),
        "filename": doc.file_name,
    }
    PENDING_FLOW[uid] = {"step": "token"}

    notes = []
    if dup_count:
        notes.append(f"🔁 Duplicate URLs (all kept): `{dup_count}`")
    if enc_skipped:
        notes.append(f"⚠️ Encrypted `AURATX://` (no key): `{enc_skipped}`")
    extra = ("\n" + "\n".join(notes)) if notes else ""

    await message.reply_text(
        f"✅ File parsed: `{doc.file_name}` (`{len(items)}` items queued){extra}\n"
        f"*(Optional: `/range 1 50` se range bhi chun sakte hain)*\n\n"
        f"🔑 Ab **Fresh User Token** bhejein (ya bina token ke aage badhne ke liye niche button dabayein):",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("⏭️ Skip Token", callback_data="skip_token")]]
        ),
    )


@app.on_message(filters.text & filters.private)
async def on_text_input(client: Client, message: Message):
    if not message.from_user:
        return
    uid = message.from_user.id
    txt = message.text.strip()

    flow = PENDING_FLOW.get(uid)
    if flow and flow.get("step") == "password":
        if txt == BOT_PASSWORD:
            PENDING_FLOW.pop(uid, None)
            mark_unlocked(uid)
            await message.reply_text("🔓 Password sahi hai! Bot unlock ho gaya.")
            await send_home(client, uid, message.chat.id)
        else:
            await message.reply_text("❌ Galat password! Dobara sahi password enter karein:")
        return

    if not is_authorized(uid):
        PENDING_FLOW[uid] = {"step": "password"}
        await message.reply_text("🔐 Pehle sahi **Password** enter karein:")
        return

    if not flow:
        return

    step = flow.get("step")

    if step == "channel":
        try:
            ch_id = int(txt)
            get_user_cfg(uid)["channel"] = ch_id
            save_db()
            PENDING_FLOW.pop(uid, None)
            await message.reply_text(f"✅ Target Channel/Group set: `{ch_id}`", reply_markup=main_menu_keyboard(uid))
        except ValueError:
            await message.reply_text("❌ Numeric Channel/Group ID bhejein (jaise `-1001234567890`).")
        return

    if step == "credit":
        get_user_cfg(uid)["credit"] = txt or DEFAULT_CREDIT
        save_db()
        PENDING_FLOW.pop(uid, None)
        await message.reply_text(f"✅ Credit set: `{get_user_cfg(uid)['credit']}`", reply_markup=main_menu_keyboard(uid))
        return

    if step == "token":
        get_user_cfg(uid)["token"] = "" if txt.lower() == "skip" else txt
        save_db()
        PENDING_FLOW.pop(uid, None)
        await message.reply_text(
            "🎯 **Upload Mode Chunein:**\n"
            "• `/channel` — Target channel/group me direct upload\n"
            "• `/grouptopictransfer` — Group me auto topic bana kar upload",
            reply_markup=mode_selection_keyboard(),
        )
        return

    if step == "prem_uid" and is_owner(uid):
        try:
            target_uid = int(txt)
            PENDING_FLOW.pop(uid, None)
            await message.reply_text(
                f"👑 User ID `{target_uid}` ke liye plan duration chunein:",
                reply_markup=premium_days_keyboard(target_uid),
            )
        except ValueError:
            await message.reply_text("❌ Valid numeric User ID bhejein.")
        return

    if step == "prem_custom_days" and is_owner(uid):
        try:
            days = int(txt)
            if days <= 0:
                raise ValueError()
            target_uid = int(flow["target_uid"])
            PENDING_FLOW.pop(uid, None)
            exp = grant_premium(target_uid, days)
            await message.reply_text(
                f"✅ Premium active!\n• User ID: `{target_uid}`\n• Duration: `{days}` days\n• Valid till: `{exp.strftime('%Y-%m-%d %H:%M UTC')}`",
                reply_markup=main_menu_keyboard(uid),
            )
        except ValueError:
            await message.reply_text("❌ 1 ya usse bada valid integer days me likhein (jaise `7` ya `45`).")
        return


@app.on_callback_query()
async def on_callback(client: Client, query: CallbackQuery):
    if not query.from_user:
        return
    uid = query.from_user.id
    data = query.data or ""

    if not is_authorized(uid):
        PENDING_FLOW[uid] = {"step": "password"}
        await query.answer("Pehle password daalein!", show_alert=True)
        await query.message.reply_text("🔐 Bot locked hai. Pehle **Password** enter karein:")
        return

    await query.answer()

    if data == "act_txt":
        PENDING_FLOW[uid] = {"step": "txt"}
        await query.message.reply_text("📄 Apni course **`.txt` file** bhejein:")
    elif data == "act_setchannel":
        PENDING_FLOW[uid] = {"step": "channel"}
        await query.message.reply_text("📢 Target **Channel/Group ID** bhejein (jaise `-1001234567890`):")
    elif data == "act_setcredit":
        PENDING_FLOW[uid] = {"step": "credit"}
        await query.message.reply_text("🏷️ Credit text bhejein (jaise `Extract by Mr.Sanskari`):")
    elif data == "act_status":
        job = ACTIVE_JOBS.get(uid)
        await query.message.reply_text(build_live_status_text(job) if job else "ℹ️ Koi active batch nahi chal raha.")
    elif data == "act_retry":
        if uid in ACTIVE_JOBS:
            await query.message.reply_text("⚠️ Ek batch pehle se chal raha hai.")
            return
        rec = FAILED_BATCH.get(uid)
        if not rec or not rec.get("items"):
            await query.message.reply_text("✅ Koi failed item pending nahi hai.")
            return
        asyncio.create_task(run_batch_job(client, uid, rec.get("mode", "channel"), custom_items=list(rec["items"])))
    elif data == "act_stop":
        job = ACTIVE_JOBS.get(uid)
        if job:
            job["cancel"] = True
            await query.message.reply_text("🛑 Batch stop signal bhej diya gaya hai.")
        else:
            await query.message.reply_text("ℹ️ Koi active batch nahi chal raha.")
    elif data == "act_lock":
        if not is_owner(uid) and get_premium_expiry(uid) is None:
            lock_user(uid)
        PENDING_FLOW[uid] = {"step": "password"}
        await query.message.reply_text("🔒 Lock ho gaya.")
    elif data == "skip_token":
        get_user_cfg(uid)["token"] = ""
        save_db()
        PENDING_FLOW.pop(uid, None)
        await query.message.reply_text(
            "🎯 **Upload Mode Chunein:**",
            reply_markup=mode_selection_keyboard(),
        )
    elif data in ("mode_channel", "mode_topic"):
        if uid not in PENDING_BATCH or not PENDING_BATCH[uid]["items"]:
            await query.message.reply_text("❌ Pehle `.txt` file upload karein.")
            return
        if uid in ACTIVE_JOBS:
            await query.message.reply_text("⚠️ Ek batch pehle se chal raha hai.")
            return
        mode = "topic" if data == "mode_topic" else "channel"
        asyncio.create_task(run_batch_job(client, uid, mode))
    elif data == "act_premium":
        if not is_owner(uid):
            await query.message.reply_text("❌ Sirf Owner hi Premium plan assign kar sakta hai.")
            return
        PENDING_FLOW[uid] = {"step": "prem_uid"}
        await query.message.reply_text("👑 Jis user ko plan dena hai uski **User ID** bhejein:")
    elif data.startswith("prem_"):
        if not is_owner(uid):
            return
        _, raw_target, raw_days = data.split("_", 2)
        target_uid = int(raw_target)
        if raw_days == "custom":
            PENDING_FLOW[uid] = {"step": "prem_custom_days", "target_uid": target_uid}
            await query.message.reply_text(f"✍️ User `{target_uid}` ke liye kitne din ka plan chahiye (number likhein)?")
        else:
            days = int(raw_days)
            exp = grant_premium(target_uid, days)
            await query.message.reply_text(
                f"✅ Premium active!\n• User ID: `{target_uid}`\n• Duration: `{days}` days\n• Valid till: `{exp.strftime('%Y-%m-%d %H:%M UTC')}`",
                reply_markup=main_menu_keyboard(uid),
            )


async def runner():
    start_health_server()
    await app.start()
    logger.info("Bot started successfully.")
    await idle()
    await app.stop()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(runner())
    finally:
        loop.close()
